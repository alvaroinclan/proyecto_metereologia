from weather.data.load import load_all_stations


def test_load_data():
    df = load_all_stations("data/raw")

    assert df.shape[0] > 0
    assert "station" in df.columns