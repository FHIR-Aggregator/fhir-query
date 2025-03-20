from fhir_aggregator_client import find_key_with_path, get_value_from_path


def test_recursive_search():
    data = {"a": {"b": {"c": 42}, "d": [{"e": 100}, {"f": 200}], "g": {"h": {"f": 300}}}}

    key_to_find = "f"
    result = find_key_with_path(data, key_to_find)
    expected = [{"path": ["a", "d", 1, "f"], "value": 200}, {"path": ["a", "g", "h", "f"], "value": 300}]
    assert result == expected, result

    ignored_keys = ["g"]
    result = find_key_with_path(data, key_to_find, ignored_keys=ignored_keys)
    expected = [{"path": ["a", "d", 1, "f"], "value": 200}]
    assert result == expected, result


def test_recursive_search_empty():
    data = {"a": {"b": {"c": 42}, "d": [{"e": 100}, {"f": 200}], "g": {"h": {"f": 300}}}}

    key_to_find = "x"
    result = find_key_with_path(data, key_to_find)
    expected = []
    assert result == expected, result


def test_get_value_from_path():
    data = {"a": {"b": {"c": 42}, "d": [{"e": 100}, {"f": 200}], "g": {"h": {"f": 300}}}}
    path = ["a", "d", 1, "f"]
    result = get_value_from_path(data, path)
    expected = 200
    assert result == expected, result


def test_group():
    true = True
    false = False
    group = {
        "resourceType": "Group",
        "id": "fam-675a7f36-8d2e-11e9-bdce-0a1683597132",
        "meta": {
            "versionId": "1",
            "lastUpdated": "2024-09-23T15:28:21.941-04:00",
            "source": "#I8dahfpmv0X4ipdQ",
            "security": [
                {"system": "http://terminology.hl7.org/CodeSystem/v3-Confidentiality", "code": "U", "display": "unrestricted"}
            ],
        },
        "identifier": [{"system": "phs001232-FamilyIdentifier", "value": "675a7f36-8d2e-11e9-bdce-0a1683597132"}],
        "type": "person",
        "actual": true,
        "name": "675a7f36-8d2e-11e9-bdce-0a1683597132",
        "member": [
            {"entity": {"reference": "Patient/1826993"}},
            {"entity": {"reference": "Patient/1827059"}},
            {"entity": {"reference": "Patient/1827006"}},
        ],
    }
    key_to_find = "reference"
    result = find_key_with_path(group, key_to_find)
    assert len(result) > 0, result
    assert sorted([_["value"] for _ in result]) == ["Patient/1826993", "Patient/1827006", "Patient/1827059"]
