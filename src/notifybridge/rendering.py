from importlib.resources import files

from jinja2 import Environment, StrictUndefined

ENV = Environment(autoescape=True, undefined=StrictUndefined)
TEMPLATE = ENV.from_string(
    files("notifybridge").joinpath("templates/notification.html").read_text()
)


def render(delivery) -> dict:
    items = delivery.items
    title = items[0]["subject"] if len(items) == 1 else f"Новых уведомлений: {len(items)}"
    return {
        "channel": delivery.channel,
        "destination": delivery.destination,
        "subject": title,
        "text": "\n\n".join(f"{item['subject']}\n{item['body']}" for item in items),
        "html": TEMPLATE.render(title=title, items=items),
        "event_ids": [item["event_id"] for item in items],
    }
