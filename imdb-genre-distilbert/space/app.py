import gradio as gr
from transformers import pipeline

MODEL = "kolodkin/imdb-genre-distilbert"

pipe = pipeline("text-classification", model=MODEL, top_k=None)


def predict(text):
    if not text or not text.strip():
        return {}
    scores = pipe(text, truncation=True, max_length=512)[0]
    return {row["label"]: float(row["score"]) for row in scores}


examples = [
    "A young wizard discovers a magical school of witchcraft and faces a dark sorcerer.",
    "Two cops in 1970s Los Angeles investigate a string of brutal murders linked to organized crime.",
    "A robot from the future is sent back in time to protect a teenager from killer machines.",
    "A struggling stand-up comedian falls in love with a journalist while touring small clubs.",
]

gr.Interface(
    fn=predict,
    inputs=gr.Textbox(lines=6, label="Plot summary", placeholder="Paste a movie or show plot..."),
    outputs=gr.Label(num_top_classes=5, label="Predicted genres"),
    title="IMDb Plot → Genre Classifier",
    description=(
        f"Multi-label genre prediction from plot text. "
        f"Model: [`{MODEL}`](https://huggingface.co/{MODEL})."
    ),
    examples=examples,
).launch()
