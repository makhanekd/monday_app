import sqlite3
from dataclasses import dataclass
from datetime import date

import dateutil.parser
import requests
from config import MONDAY_API_KEY


@dataclass(slots=True)
class Task:
    id: int
    name: str
    label: str
    priority_text: str
    people: str
    date: date | None
    project_status: str
    effort: int | None
    relevant: bool


class QueryHander:
    def __init__(self, conn):
        self.conn = conn

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                label TEXT NOT NULL,
                priority_text TEXT NOT NULL,
                people TEXT NOT NULL,
                date DATE,
                project_status TEXT NOT NULL,
                effort INTEGER,
                relevant BOOLEAN NOT NULL
            )
        ''')
        self.conn.commit()

    def insert_or_update_task(self, task: Task):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO tasks (id, name, label, priority_text, people, date, project_status, effort, relevant)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                label=excluded.label,
                priority_text=excluded.priority_text,
                people=excluded.people,
                date=excluded.date,
                project_status=excluded.project_status,
                effort=excluded.effort,
                relevant=excluded.relevant
        ''', (task.id, task.name, task.label, task.priority_text, task.people, task.date, task.project_status, task.effort, task.relevant))
        self.conn.commit()


class MondayHandler:
    def __init__(self) -> None:
        self.api_key = MONDAY_API_KEY
        self.api_url = "https://api.monday.com/v2/"
        self.headers = {
            "Authorization": self.api_key,
            "API-Version": '2023-07',
            "Content-Type": "application/json"
        }

    def get_data(self):
        tasks: list[Task] = []
        body = """
            query {
                boards (ids: 1385666653) {
                    items_page {
                        cursor items {
                            id
                            name
                            column_values (
                                ids: [
                                    "priority_1", "people", "label", "date", "project_status",
                                    "numbers__1", "status_1__1"
                                ]
                            ) {id text}
                        }
                    }
                }
            }
        """
        response = requests.get(self.api_url, headers=self.headers, json={"query": body})
        data_raw = response.json()
        tasks = self.get_format_data(data_raw, is_first_req=True)


        cursor = data_raw['data']['boards'][0]['items_page']['cursor']
        while cursor is not None:
            body = f"""
                query {{
                    next_items_page (cursor: "{cursor}") {{
                        cursor items {{
                            id
                            name
                            column_values (
                                ids: ["priority_1","people","label", "date", "project_status", "numbers__1", "status_1__1"]
                            ) {{id text}}
                        }}
                    }}
                }}
            """
            response = requests.get(self.api_url, headers=self.headers, json={"query": body})
            data_raw = response.json()
            print(data_raw)
            cursor = data_raw['data']['next_items_page']['cursor']
            tasks = tasks + self.get_format_data(data_raw)

        conn = sqlite3.connect('tasks.db')
        qh = QueryHander(conn)
        qh.create_table()
        for task in tasks:
            qh.insert_or_update_task(task)

        conn.close()

    def get_format_data(self, data_raw: dict, is_first_req: bool=False):
        tasks: list[Task] = []
        items = data_raw['data']['boards'][0]['items_page']['items'] if is_first_req else data_raw['data']['next_items_page']['items']
        for item in items:
            tasks.append(Task(
                id=item['id'],
                name=item['name'],
                label=next(cv['text'] for cv in item['column_values'] if cv['id'] == 'label'),
                priority_text=next(cv['text'] for cv in item['column_values'] if cv['id'] == 'priority_1'),
                people=next(cv['text'] for cv in item['column_values'] if cv['id'] == 'people'),
                date=self._parse_date(next(cv['text'] for cv in item['column_values'] if cv['id'] == 'date')),
                project_status=next(cv['text'] for cv in item['column_values'] if cv['id'] == 'project_status'),
                effort=self._parse_effort(next(cv['text'] for cv in item['column_values'] if cv['id'] == 'numbers__1')),
                relevant=self._parse_relevant(next(cv['text'] for cv in item['column_values'] if cv['id'] == 'status_1__1'))
            ))
        return tasks

    def _parse_date(self, date_str: str) -> date | None:
        if date_str:
            return dateutil.parser.parse(date_str).date()
        return None

    def _parse_effort(self, effort_str: str) -> int | None:
        if effort_str:
            return int(effort_str)
        return None

    def _parse_relevant(self, relevant_str: str) -> bool:
        return relevant_str.lower() == 'yes'


def main():
    mh = MondayHandler()
    mh.get_data()


if __name__ == "__main__":
    main()
