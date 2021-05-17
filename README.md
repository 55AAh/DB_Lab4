Датасети необхідно покласти в папку ```populate/data```.

Роботу можна розгорнути в ```docker-compose```:
```shell
docker-compose run --rm populate
```

Або запустити скрипт в ручному режимі:
```shess
cd populate
python main.py
```

* Параметри підключення до бази даних - в ```db-auth.env```;
* Параметри роботи скрипта - в ```populate_conf.env```;

Результат виконання запиту знаходиться в папці ```populate```.