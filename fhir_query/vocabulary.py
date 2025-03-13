from urllib.parse import urlparse, urlencode, quote_plus

import inflection


def _get_path(component):
    """Get the vocabulary path from the component."""
    for coding in component.get("code", {}).get("coding", []):
        if coding.get("system", "") == "http://fhir-aggregator.org/fhir/CodeSystem/vocabulary/path":
            return coding.get("code", None)


def _get_coding(component):
    """Get the vocabulary codeable from the component."""
    for coding in component.get("code", {}).get("coding", []):
        if coding.get("system", "") != "http://fhir-aggregator.org/fhir/CodeSystem/vocabulary/path":
            return coding


def vocabulary_simplifier(bundle) -> list[dict]:
    """Simplify the vocabulary bundle."""
    df = []
    base_url = bundle.get("link", [{}])[0].get("url", "")
    assert base_url, "No base url found"
    parsed_url = urlparse(base_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    resources = {f'{r["resource"]["resourceType"]}/{r["resource"]["id"]}': r["resource"] for r in bundle.get("entry", [])}
    for _id, resource in resources.items():
        if resource["resourceType"] != "Observation":
            continue
        focus = next(iter(resource.get("focus", [])), None)
        assert focus, f"No focus found for Observation {resource['id']}"
        focus_reference = focus.get("reference", None)
        research_study = resources.get(focus_reference, None)
        assert research_study, f"No research_study reference found for Observation {resource['id']} {focus_reference}"
        for component in resource.get("component", []):
            path = _get_path(component)
            path_resource, element = path.split(".")

            # get the documentation link for this path
            doc_url = f"https://hl7.org/fhir/R4B/{path_resource.lower()}-definitions.html#{path}"

            # TODO this is a hack to get the name of the SearchParameter from the element name
            # change element from camelCase to dash-case
            element = inflection.dasherize(inflection.underscore(element))

            code_filter = None
            coding = _get_coding(component)
            item = {
                "research_study_identifiers": ",".join([i.get("value", "") for i in research_study.get("identifier", [])]),
                "path": path,
                "documentation": doc_url,
            }
            if path.endswith(".extension"):
                item.update(
                    {
                        "code": coding.get("code", None) if coding.get("code", None) != "range" else None,
                        "display": coding.get("display", None) if coding.get("display", None) != "range" else None,
                        "system": None,
                        "extension_url": coding.get("system", None),
                    }
                )
                # TODO this is a hack to get the name of the SearchParameter from the extension url
                element = item["extension_url"].split("-")[-1]
            else:
                item.update(
                    {
                        "code": coding.get("code", None),
                        "display": coding.get("display", None),
                        "system": coding.get("system", None),
                        "extension_url": None,
                    }
                )
            if item["code"]:
                code_filter = f"{element}={quote_plus(str(item['code']))}"

            if "valueInteger" in component:
                item["count"] = component["valueInteger"]
            else:
                item["count"] = None
            if "valueRange" in component:
                item["low"] = component["valueRange"].get("low", {}).get("value", None)
                item["high"] = component["valueRange"].get("high", {}).get("value", None)
            else:
                item["low"] = None
                item["high"] = None

            url = base_url + f"/{path_resource}?{code_filter}&part-of-study=ResearchStudy/{research_study['id']}"
            if code_filter:
                item["url"] = url
            else:
                item["url"] = None

            item.update(
                {
                    "research_study_title": research_study.get("title", None),
                    "research_study_description": research_study.get("description", None),
                    "observation": f'Observation/{resource["id"]}',
                    "research_study": f'ResearchStudy/{research_study["id"]}',
                }
            )

            df.append(item)
    return df
