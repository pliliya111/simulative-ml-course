import json
from pathlib import Path


class JsonFileStorage:
    """Чтение/запись состояния в JSON-файл."""

    def __init__(self, file_path: str):
        self._path = Path(file_path)

    def load(self) -> dict:
        if self._path.exists():
            return json.loads(self._path.read_text(encoding="utf-8"))
        return {}

    def save(self, data: dict) -> None:
        self._path.write_text(
            json.dumps(data, ensure_ascii=False, default=str, indent=2),
            encoding="utf-8",
        )


class State:
    """Обёртка над хранилищем: get/set по ключу с авто-сохранением."""

    def __init__(self, storage: JsonFileStorage):
        self._storage = storage
        self._state = storage.load()

    def get_state(self, key: str):
        return self._state.get(key)

    def set_state(self, key: str, value) -> None:
        self._state[key] = value
        self._storage.save(self._state)
