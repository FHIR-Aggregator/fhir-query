from fhir_query import find_key_with_path, get_value_from_path


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
