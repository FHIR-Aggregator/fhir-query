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
            coding = _get_coding(component)
            item = {
                "research_study_identifiers": ",".join([i.get("value", "") for i in research_study.get("identifier", [])]),
                "path": path,
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
            else:
                item.update(
                    {
                        "code": coding.get("code", None),
                        "display": coding.get("display", None),
                        "system": coding.get("system", None),
                        "extension_url": None,
                    }
                )

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
