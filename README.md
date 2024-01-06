Due to time constraints, the codes are being organized and will be uploaded completely before January 10, 2024.

We use mysql to store data for a better view.

Requirements:
- mysql 5.7
- python 3.7
    - pip install mysql-connector-python


Data:
- [data/city_trace](./data/city_trace/): traceroute data for 26 countries. mysql dump files. Download->Decompress->Load into your mysql.
- [data/city_landmarks](./data/city_landmark/): We randomly select 20% of the targets in 'city_trace' as landmarks for 10 trials. This forder contains the selected landmarks. You need not load these data to your mysql. They are unnecessary for running the codes. Each dump file contains 10 tables for a country.
- [data/subgraphs](./data/subgraphs/): IPv6 addresses in each subgraphs (clusters).
- [data/accuracy/accuracy.dhc8.lmp20.10rounds.txt](./data/accuracy/accuracy.dhc8.lmp20.10rounds.txt) provides historical city-level geolocation accuracy by using [data/city_landmarks](./data/city_landmark/) and [data/subgraphs](./data/subgraphs/).

| **Name**    | **Type** | **length** | **Descript**                        |
|-------------|----------|------------|-------------------------------------|
| id          | int      | 10         | primary key, untoincrement          |
| src         | varchar  | 47         | source IP/vantage ponint IP address |
| dst         | varchar  | 47         | destination for a path              |
| path_id     | int      | 10         | same for a path                     |
| stop_reason | varchar  | 50         | Scamper stop reason                 |
| ip          | varchar  | 47         | router interface in a path          |
| rtt         | double   | 16         | round trip time                     |
| hop_id      | int      | 10         | TTL/hop count                       |
| country     | varchar  | 50         | country of dst                      |
| province    | varchar  | 50         | province of dst                     |
| city        | varchar  | 50         | city of dst                         |
| district    | varchar  | 50         | district of dst. not used           |
| detail      | varchar  | 50         | detail information. not used        |
| isp         | varchar  | 200        | isp of dst. not used                | 


Codes:
- tables.py: table creation statements.
- [city_geo.py](./city_geo.py): GDD-CGeo.

To retest our geolocation results, use the function `geo_city3_test_aveAcc10rounds()`. It will randomly select 20% IPv6 addresses as landmarks and the rest 80% as targets for 10 times and then geolocate the targets. It will provide an overview of city-level geolocation accuracy for each country in [data/accuracy/accuracy.dhc8.lmp20.10rounds.txt](./data/accuracy/accuracy.dhc8.lmp20.10rounds.txt). eg,
```python
if __name__ == '__main__':
    geo_city3_test_aveAcc10rounds()
```

To select 30% IPv6 addresses as landmarks and geolocate the targets in china, use the function `geo_instance()`. eg,
```python
if __name__ == '__main__':
    geo_instance(n=0.3,country='china')
```

