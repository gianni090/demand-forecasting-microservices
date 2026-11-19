# Лабораторная работа № 5. CI/CD для микросервисов

Пайплайн обрабатывает три сервиса системы прогнозирования спроса из ЛР № 4. Конфигурация хранится в [.github/workflows/lab5-cicd.yml](../.github/workflows/lab5-cicd.yml), результаты запусков доступны во вкладке [GitHub Actions](https://github.com/gianni090/demand-forecasting-microservices/actions).

## Почему GitHub Actions

Код и pull request уже находятся на GitHub. Actions запускает задания на готовых Linux-раннерах и даёт доступ к журналу каждого шага. Матрица позволяет проверять три сервиса параллельно одним workflow. Для публикации в GitHub Container Registry (GHCR) используется встроенный `GITHUB_TOKEN`. [Документация GitHub](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images)

| Сервис | Что проверяет unit-тест |
| --- | --- |
| `sales-service` | Импорт CSV агрегирует продажи по неделям, исключает возвраты из объёма и учитывает строки с ошибками. |
| `forecast-service` | Методы Last Value, TSB и LightGBM дают упорядоченные квантили и сохраняют данные модели. LightGBM требует достаточной истории. |
| `inventory-service` | После отмены плана склад отклоняет позднюю попытку создать план для того же заказа. |

## Что делает пайплайн

1. **Сборка.** Раннер устанавливает Python 3.12 и зависимости из `requirements.txt`. Действие настройки Python кэширует загрузки pip. `compileall` проверяет, что исходные модули компилируются.
2. **Unit-тесты.** Каждый элемент матрицы запускает свой тест из `tests/test_domain.py`. Используются небольшие тестовые данные из кода.
3. **Упаковка.** Общий `Dockerfile` принимает параметр `SERVICE`. Команда запуска поднимает выбранный сервис. Раннер запускает контейнер и проверяет, что `GET /healthz` возвращает имя нужного сервиса и статус `ok`.
4. **Публикация.** Проверенный образ продаж передаётся между заданиями как архив. После успешного завершения всей матрицы задание CD загружает образ и публикует его с тегом `sha-<полный SHA коммита>`. Имя опубликованного образа выводится в Summary запуска.

CI запускается при push в `main`, в ветку реализации `lab5/ci-cd` и в демонстрационную ветку `lab5/ci-failure`. Также CI запускается для pull request в `main`. Вручную его можно запустить, когда workflow появится в основной ветке. CD запускается после push в `main` или `lab5/ci-cd`. На этапе CD токен получает `packages: write`. Для заданий CI достаточно `contents: read`. GitHub выдаёт токен на время задания, дополнительных секретов для GHCR не требуется.

Контейнеры собираются отдельно. Сквозной сценарий пяти сервисов и Gateway по-прежнему запускается командой `python -m services.runner`. В ЛР № 5 результат CD — контейнерный образ в реестре.

## Проверка на компьютере

В корне репозитория установите зависимости и запустите тесты. Нужны Python 3.12 и Docker Desktop для проверки контейнеров.

```powershell
python -m pip install -r requirements.txt
python -m compileall -q services mocks
python -m unittest discover -s tests -v
```

Для каждого образа повторите сборку с соответствующим именем. Затем можно запустить контейнер продаж и проверить его адрес.

```powershell
docker build --build-arg SERVICE=sales-service -t sales-service:ci .
docker build --build-arg SERVICE=forecast-service -t forecast-service:ci .
docker build --build-arg SERVICE=inventory-service -t inventory-service:ci .
docker run --rm -d --name lab5-sales -p 127.0.0.1:18080:8000 sales-service:ci
Invoke-RestMethod http://127.0.0.1:18080/healthz
docker stop lab5-sales
```

Успешный ответ содержит поля `status` со значением `ok` и `service` со значением `sales-service`. Для опубликованного образа скопируйте адрес из Summary задания CD. Если пакет закрыт, для загрузки требуется доступ к пакету и вход в GHCR.

## Демонстрация обработки ошибки

В демонстрационной ветке `lab5/ci-failure` меняется ожидаемый результат unit-теста продаж. Этот запуск показывает, что при ошибке теста GitHub пропускает сборку образа продаж. Остальные элементы матрицы выполняются независимо. Изменение теста остаётся в демонстрационной ветке.
