from dagster_project.assets.synthetic_data import employees

def test_employees_asset():
    df = employees()
    assert not df.empty
    assert "salary" in df.columns