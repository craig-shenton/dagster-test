from dagster_project.assets.raw_assets import employees

def test_employees_asset():
    df = employees()
    assert not df.empty
    assert "salary" in df.columns