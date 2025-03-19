import json
from collections import defaultdict
from functools import lru_cache
from typing import Generator

from fhir_aggregator_client import ResourceDB

import inflection

from pydantic import BaseModel, computed_field
from typing import Dict, List, Optional, Tuple


#######################
# FHIR HELPER METHODS #
#######################


def get_nested_value(d: dict, keys: list):
    for key in keys:
        try:
            d = d[key]
        except (KeyError, IndexError, TypeError):
            return None
    return d


def normalize_coding(resource_dict: Dict) -> List[Tuple[str, str]]:
    """normalize any nested coding"""

    def extract_coding(coding_list):
        # return a concatenated string
        # or alternatively return an array
        return [coding.get("display", coding.get("code", "")) for coding in coding_list]

    def find_codings_in_dict(d: dict, parent_key: str = "") -> list[tuple[str, str]]:  # TODO - parent_key not used?
        codings = []
        for key, value in d.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        # Check if the dict contains a 'coding' list
                        if "coding" in item and isinstance(item["coding"], list):
                            coding_string = extract_coding(item["coding"])
                            codings.append((coding_string, key))
                        if "code" in item:
                            coding_string = item.get("display", item.get("code"))
                            codings.append((coding_string, key))

                        # Recursively search in the dict
                        codings.extend(find_codings_in_dict(item, key))
            elif isinstance(value, dict):
                # Check if the dict contains a 'coding' list
                if "coding" in value and isinstance(value["coding"], list):
                    coding_string = extract_coding(value["coding"])
                    codings.append((coding_string, key))

                # Recursively search in the dict
                codings.extend(find_codings_in_dict(value, key))
        return codings

    return find_codings_in_dict(resource_dict)


def normalize_value(resource_dict: dict) -> tuple[Optional[str], Optional[str]]:
    """return a tuple containing the normalized value and the name of the field it was derived from"""

    if "valueQuantity" in resource_dict:
        value = resource_dict["valueQuantity"]
        value_normalized = f"{value['value']} {value.get('unit', '')}"
        value_source = "valueQuantity"
    elif "valueCodeableConcept" in resource_dict:
        value = resource_dict["valueCodeableConcept"]
        value_normalized = " ".join([coding.get("display", coding.get("code", "")) for coding in value.get("coding", [])])
        value_source = "valueCodeableConcept"
    elif "valueCoding" in resource_dict:
        value = resource_dict["valueCoding"]
        value_normalized = value["display"]
        value_source = "valueCoding"
    elif "valueString" in resource_dict:
        value_normalized = resource_dict["valueString"]
        value_source = "valueString"
    elif "valueCode" in resource_dict:
        value_normalized = resource_dict["valueCode"]
        value_source = "valueCode"
    elif "valueBoolean" in resource_dict:
        value_normalized = str(resource_dict["valueBoolean"])
        value_source = "valueBoolean"
    elif "valueInteger" in resource_dict:
        value_normalized = str(resource_dict["valueInteger"])
        value_source = "valueInteger"
    elif "valueRange" in resource_dict:
        value = resource_dict["valueRange"]
        low = value["low"]
        high = value["high"]
        value_normalized = f"{low['value']} - {high['value']} {low.get('unit', '')}"
        value_source = "valueRange"
    elif "valueRatio" in resource_dict:
        value = resource_dict["valueRatio"]
        numerator = value["numerator"]
        denominator = value["denominator"]
        value_normalized = f"{numerator['value']} {numerator.get('unit', '')}/{denominator['value']} {denominator.get('unit', '')}"
        value_source = "valueRatio"
    elif "valueSampledData" in resource_dict:
        value = resource_dict["valueSampledData"]
        value_normalized = value["data"]
        value_source = "valueSampledData"
    elif "valueTime" in resource_dict:
        value_normalized = resource_dict["valueTime"]
        value_source = "valueTime"
    elif "valueDateTime" in resource_dict:
        value_normalized = resource_dict["valueDateTime"]
        value_source = "valueDateTime"
    elif "valuePeriod" in resource_dict:
        value = resource_dict["valuePeriod"]
        value_normalized = f"{value['start']} to {value['end']}"
        value_source = "valuePeriod"
    elif "valueUrl" in resource_dict:
        value_normalized = resource_dict["valueUrl"]
        value_source = "valueUrl"
    elif "valueDate" in resource_dict:
        value_normalized = resource_dict["valueDate"]
        value_source = "valueDate"
    elif "valueCount" in resource_dict:
        value_normalized = resource_dict["valueCount"]["value"]
        value_source = "valueCount"
    elif "valueReference" in resource_dict:
        value_normalized = resource_dict["valueReference"]["reference"]
        value_source = "valueReference"

    else:
        value_normalized, value_source = None, None
        # for debugging...
        # raise ValueError(f"value[x] not found in {resource_dict}")

    return value_normalized, value_source


def normalize_for_guppy(key: str):
    """normalize a key so that it can be loaded into Guppy as a column name"""
    guppy_table = str.maketrans(
        {
            ".": "",
            " ": "_",
            "[": "",
            "]": "",
            "'": "",
            ")": "",
            "(": "",
            ",": "",
            "/": "_per_",
            "-": "to",
            "#": "number",
            "+": "_plus_",
            "%": "percent",
            "&": "_and_",
        }
    )
    return key.translate(guppy_table)


def traverse(resource):
    """simplify a resource's fields, returned as a dict of values,
    where keys are prefixed with "resourceType_" """

    final_subject = {}
    simplified_subject = SimplifiedResource.build(resource=resource).simplified
    prefix = simplified_subject["resourceType"].lower()
    for k, v in simplified_subject.items():
        if k in ["resourceType"]:
            continue
        final_subject[f"{prefix}_{k}"] = v

    return final_subject


########################
# FHIR BUILDER OBJECTS #
########################


class SimplifiedFHIR(BaseModel):
    """All Simplifiers should inherit from this class."""

    warnings: list[str] = []
    """A list of warnings generated during the simplification process."""
    resource: dict
    """The FHIR resource to be simplified."""

    @computed_field()  # type: ignore[prop-decorator]
    @property
    def simplified(self) -> dict:
        _ = self.identifiers.copy() if self.identifiers else {}
        _.update(self.scalars)
        _.update(self.codings)
        _.update(self.extensions)
        _.update(self.values)
        return _

    def simplify_extensions(self, resource: dict = None, _extensions: dict = None) -> dict:  # type: ignore  # noqa
        """Extract extension values, derive key from extension url"""

        def _populate_simplified_extension(extension: dict):
            # simple extension
            value_normalized, extension_key = normalize_value(extension)
            extension_key = extension["url"].split("/")[-1]
            extension_key = inflection.underscore(extension_key).removesuffix(".json").removeprefix("structure_definition_")  # type: ignore[arg-type]
            if value_normalized is None:
                pass
            assert value_normalized is not None, f"extension: {extension_key} = {value_normalized} {extension}"
            _extensions[extension_key] = value_normalized

        if not _extensions:
            _extensions = {}

        if not resource:
            resource = self.resource

        if "extension" not in resource.keys():
            return _extensions

        for _ in resource.get("extension", [resource]):
            if "extension" not in _.keys():
                if "resourceType" not in _.keys():
                    _populate_simplified_extension(_)
                continue
            elif set(_.keys()) == {"url", "extension"}:
                for child_extension in _["extension"]:
                    self.simplify_extensions(resource=child_extension, _extensions=_extensions)

        return _extensions

    @computed_field  # type: ignore[prop-decorator]
    @property
    def extensions(self) -> dict:
        return self.simplify_extensions()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def scalars(self) -> dict:
        """Return a dictionary of scalar values."""
        return {k: v for k, v in self.resource.items() if (not isinstance(v, list) and not isinstance(v, dict))}

    @computed_field  # type: ignore[prop-decorator]
    @property
    def codings(self) -> dict:
        """Return a dictionary of scalar values."""
        _codings = {}
        for k, v in self.resource.items():
            # these are handled in separate methods
            if k in ["identifier", "extension", "component", "code"]:
                continue
            elif isinstance(v, list):
                for elem in v:
                    if isinstance(elem, dict):
                        # TODO: implement hierarchy of codes rather than just taking last code?
                        for value, source in normalize_coding(elem):
                            if len(v) > 1 and get_nested_value(elem, [source, 0, "system"]):
                                _codings[elem[source][0]["system"].split("/")[-1]] = value
                            else:
                                _codings[k] = value
            elif isinstance(v, dict):
                for value, elem in normalize_coding(v):
                    _codings[k] = value

        return _codings

    @computed_field  # type: ignore[prop-decorator]
    @property
    def identifiers(self) -> dict:
        """Return the first of a resource and any other resources"""
        identifiers = self.resource.get("identifier", [])
        identifiers_len = len(identifiers)

        if not identifiers_len:
            return {"identifier": None}
        elif identifiers_len == 1:
            return {"identifier": identifiers[0].get("value")}
        else:
            base_identifier = {"identifier": identifiers[0].get("value")}
            base_identifier.update(
                {identifier.get("system").split("/")[-1]: identifier.get("value") for identifier in identifiers[1:]}
            )
            return base_identifier

    @computed_field  # type: ignore[prop-decorator]
    @property
    def values(self) -> dict:
        """Return a dictionary of source:value."""
        # FIXME: values that are scalars are processed twice: once in scalars once here in values (e.g. valueString)
        value, source = normalize_value(self.resource)
        if not value:
            return {}

        # update the key if code information is available

        if self.resource.get("code", {}).get("text", None):
            source = self.resource["code"]["text"]
        else:
            source = self.resource["code"]["coding"][0].get("display", self.resource["code"]["coding"][0].get("code"))
        if not source:
            source = "NA"
        source = inflection.underscore(inflection.parameterize(source))
        return {source: value}


class SimplifiedObservation(SimplifiedFHIR):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def codings(self) -> dict:
        """does everything but gets rid of code since that's dealt with in values"""
        _codings = super().codings
        if "code" in _codings:
            del _codings["code"]

        return _codings

    # TODO: remove after data fix
    @computed_field  # type: ignore[prop-decorator]
    @property
    def scalars(self) -> dict:
        """Return a dictionary of scalar values."""
        return {
            k: v
            for k, v in self.resource.items()
            if (not isinstance(v, list) and not isinstance(v, dict) and not k == "valueString")
        }

    @computed_field  # type: ignore[prop-decorator]
    @property
    def values(self) -> dict:
        """Return a dictionary of 'value':value or <component>:value.
        https://build.fhir.org/observation-definitions.html#Observation.component
        """

        # get top-level value in dict if it exists
        _values = super().values
        if len(_values) == 0:
            assert "component" in self.resource, "no component nor top-level value found"

            # get component codes
            if "component" in self.resource:
                for component in self.resource["component"]:
                    value, source = normalize_value(component)
                    if component.get("code", {}).get("text", None):
                        source = component["code"]["text"]
                    if not value:
                        continue
                    _values[source] = value

            # knowing there's now at least 1 item in _values
            if "component" in self.resource:
                # ensure no top-level value is not duplicating a component code value
                # TODO: ensure this value_key corresponds to percent_tumor on some runs due to getting display
                value_key = [k for k in _values][0]
                assert (
                    value_key not in self.resource["component"]
                ), """duplicate code value found, only specify the code value in the component, see Rule obs-7
                    https://build.fhir.org/observation.html#invs"""

                # get component codes
                if "component" in self.resource:
                    for component in self.resource["component"]:
                        value, source = normalize_value(component)
                        if component.get("code", {}).get("text", None):
                            source = component["code"]["text"]
                        if not value:
                            continue
                        _values[source] = value

        if "code" in self.resource and "text" in self.resource["code"]:
            _values["observation_code"] = self.resource["code"]["text"]

        assert not [_ for _ in _values.keys() if _.startswith("value")], f"key misnamed {_values}\n  {self.resource}"
        assert len(_values) > 0, f"no values found in Observation: {self.resource}"

        return _values


class SimplifiedDocumentReference(SimplifiedFHIR):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def values(self) -> dict:
        """Return a dictionary of 'value':value."""
        _values = super().values
        for content in self.resource.get("content", []):
            if "attachment" in content:
                for k, v in SimplifiedFHIR(resource=content["attachment"]).simplified.items():
                    if k in ["identifier", "extension"]:
                        continue
                    _values[k] = v
        return _values


class SimplifiedCondition(SimplifiedFHIR):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def codings(self) -> dict:
        # only go through the work if code exists
        if "code" not in self.resource:
            return {}

        # get field name
        codings_dict = super().codings
        if "cate.g.ory" in codings_dict:
            key = codings_dict["cate.g.ory"]
            del codings_dict["cate.g.ory"]
        else:
            key = "code"

        # TODO: implement hierarchy of codes rather than just taking last code?
        value, _ = normalize_coding(self.resource["code"])[-1]
        return {key: value}


class SimplifiedResource(object):
    """A simplified FHIR resource, a factory method."""

    @staticmethod
    def build(resource: dict) -> SimplifiedFHIR:
        """Return a simplified FHIR resource."""

        resource_type = resource.get("resourceType", None)
        if resource_type == "Observation":
            return SimplifiedObservation(resource=resource)
        if resource_type == "DocumentReference":
            return SimplifiedDocumentReference(resource=resource)
        if resource_type == "Condition":
            return SimplifiedCondition(resource=resource)
        return SimplifiedFHIR(resource=resource)


class Dataframer(ResourceDB):
    def __init__(self, db_path: str):
        super().__init__(db_path)

    def get_subject(self, resource: dict) -> dict:
        """get the resource's subject field if it exists"""

        # ensure resource has subject field
        subject_key = get_nested_value(resource, ["subject", "reference"])
        if subject_key is None:
            return {}

        # traverse the resource of the subject and return its values
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?", (subject_key,))
        row = cursor.fetchone()
        assert row, f"{subject_key} not found in database"
        _, _, _, raw_subject = row
        subject = json.loads(raw_subject)
        return traverse(subject)

    def get_resources_by_reference(self, resource_type: str, reference_field: str, reference_type: str) -> dict[str, list]:
        """Given a set of resources of type resource_type, map each unique reference in reference field of type reference_type to its associated resources
        ex: use all Observations with a Specimen focus, map Specimen IDs to its list of associated Observations and return the map
        """

        # ensure reference field is allowed
        allowed_fields = ["focus", "subject", "specimen", "basedOn"]
        assert reference_field in allowed_fields, f"Field not implemented, choose between {allowed_fields}"

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT *
            FROM resources
            WHERE resource_type = ?
        """,
            (resource_type,),
        )

        resource_by_reference_id = defaultdict(list)

        for _, _, _, raw_resource in cursor.fetchall():
            resource = json.loads(raw_resource)

            # determine which how to process the field
            if reference_field == "focus" and "focus" in resource:
                # add the resource (e.g. observation) for each focus reference to the dict
                for i in range(len(resource["focus"])):
                    reference_key: str = get_nested_value(resource, [reference_field, i, "reference"])
                    if reference_key is not None and reference_type in reference_key:
                        reference_id = reference_key.split("/")[-1]
                        resource_by_reference_id[reference_id].append(resource)

            if reference_field == "specimen" and "specimen" in resource:
                # add the resource (e.g. observation) for each specimen reference to the dict
                for i in range(len(resource["specimen"])):
                    reference_key = get_nested_value(resource, [reference_field, i, "reference"])
                    if reference_key is not None and reference_type in reference_key:
                        reference_id = reference_key.split("/")[-1]
                        resource_by_reference_id[reference_id].append(resource)

            if reference_field == "basedOn" and "basedOn" in resource:
                # add the resource (e.g. observation) for each basedOn reference to the dict
                for i in range(len(resource["basedOn"])):
                    reference_key = get_nested_value(resource, [reference_field, i, "reference"])
                    if reference_key is not None and reference_type in reference_key:
                        reference_id = reference_key.split("/")[-1]
                        resource_by_reference_id[reference_id].append(resource)

            elif reference_field == "subject":
                # add the resource (e.g. observation) to the dict
                reference_key = get_nested_value(resource, [reference_field, "reference"])
                if reference_key is not None and reference_type in reference_key:
                    reference_id = reference_key.split("/")[-1]
                    resource_by_reference_id[reference_id].append(resource)

        return resource_by_reference_id

    def get_observations_by_focus(self, focus_type: str) -> dict[str, list]:
        """get all Observations that have a focus of resource type focus_type"""
        return self.get_resources_by_reference("Observation", "focus", focus_type)

    @lru_cache(maxsize=None)
    def flattened_specimens(self) -> Generator[dict, None, None]:
        """generator that yields specimens populated with `Specimen.subject` fields
        and Observation codes through `Observation.focus`"""

        resource_type = "Specimen"
        cursor = self.connection.cursor()

        # get a dict mapping focus ID to its associated observations
        specimen_observations_by_focus_id = self.get_observations_by_focus(resource_type)
        service_requests_by_specimen_id = self.get_resources_by_reference("ServiceRequest", "specimen", "Specimen")
        document_references_by_based_on_id = self.get_resources_by_reference("DocumentReference", "basedOn", "ServiceRequest")

        # flatten each document reference
        cursor.execute("SELECT * FROM resources where resource_type = ?", (resource_type,))
        for _, _, _, resource in cursor.fetchall():
            specimen = json.loads(resource)
            yield self.flattened_specimen(
                specimen, specimen_observations_by_focus_id, service_requests_by_specimen_id, document_references_by_based_on_id
            )

    def flattened_specimen(
        self, specimen: dict, observation_by_id: dict, service_requests_by_specimen_id, document_references_by_based_on_id
    ) -> dict:
        """Return the specimen with everything resolved."""

        # create simple specimen dict
        flat_specimen = traverse(specimen)

        # extract its .subject and append its fields (including id)
        subject = self.get_subject(specimen)
        if "patient_id" in subject:
            assert len(self.flattened_patients()) > 1, f"Length of flattened_patients is {len(self.flattened_patients())}"
            _flattened_patient = next(
                iter([_ for _ in self.flattened_patients() if _["patient_id"] == subject["patient_id"]]), None
            )
            if not _flattened_patient:
                print(f"Patient not found {subject['patient_id']} {[_['patient_id'] for _ in self.flattened_patients()]}")
            else:
                subject = {f"patient_{k}".replace("patient_patient_", "patient_"): v for k, v in _flattened_patient.items()}
        flat_specimen.update(subject)

        # populate observation codes for each associated observation
        if specimen["id"] in observation_by_id:
            observations = observation_by_id[specimen["id"]]
            # TODO: assumes there are no duplicate column names in each observation
            for observation in observations:
                flat_observation = SimplifiedResource.build(resource=observation).values
                flat_observation = {f"observation_{k}": v for k, v in flat_observation.items()}
                flat_specimen.update(flat_observation)

        if specimen["id"] in service_requests_by_specimen_id:
            service_requests = service_requests_by_specimen_id[specimen["id"]]
            # TODO: assumes there are no duplicate column names in each observation
            for service_request in service_requests:
                flat_specimen.update(traverse(service_request))
                if service_request["id"] in document_references_by_based_on_id:
                    document_references = document_references_by_based_on_id[service_request["id"]]
                    for document_reference in document_references:
                        flat_specimen.update(traverse(document_reference))

        return flat_specimen

    @lru_cache(maxsize=None)
    def flattened_patients(self) -> list[dict]:
        """
        Generator that yields flattened Patient records.
        Each flattened Patient merges in fields from:
            - Observations that reference the Patient via the focus field
        """
        resource_type = "Patient"
        cursor = self.connection.cursor()

        observations_by_focus = self.get_resources_by_reference("Observation", "focus", "Patient")
        cursor.execute("SELECT * FROM resources WHERE resource_type = ?", (resource_type,))
        _flattened_patients = []
        for _, _, _, resource in cursor.fetchall():
            patient = json.loads(resource)
            _flattened_patients.append(self.flattened_patient(patient, observations_by_focus))
        return _flattened_patients

    @staticmethod
    def flattened_patient(patient: dict, observations_by_subject: dict) -> dict:
        """Return the flattened Patient record with related Observations"""
        flat_patient = traverse(patient)

        if patient["id"] in observations_by_subject:
            observations = observations_by_subject[patient["id"]]
            for observation in observations:
                flat_observation = SimplifiedResource.build(resource=observation).values
                flat_observation = {f"observation_{k}": v for k, v in flat_observation.items()}
                flat_patient.update(flat_observation)

        return flat_patient
