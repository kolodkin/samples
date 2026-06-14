import gradio as gr
from transformers import pipeline

MODEL = "kolodkin/imdb-genre-distilbert"

pipe = pipeline("text-classification", model=MODEL, top_k=None)
MAX_TOKENS = pipe.tokenizer.model_max_length  # 512 for DistilBERT


def count_tokens(text):
    if not text or not text.strip():
        return 0
    return len(pipe.tokenizer(text, truncation=False)["input_ids"])


def on_change(text):
    """As the user types, flip the button to a Truncate action and show an inline
    message when the plot exceeds the model's token limit."""
    n_tokens = count_tokens(text)
    if n_tokens > MAX_TOKENS:
        msg = (
            f"⚠️ Plot is ~{n_tokens} tokens but the model reads only the first "
            f"{MAX_TOKENS}. Click **Truncate & classify** to use the opening "
            f"{MAX_TOKENS} tokens, or shorten the text for a result that reflects "
            f"the whole plot."
        )
        return (
            gr.update(value="Truncate & classify", variant="stop"),
            gr.update(value=msg, visible=True),
        )
    return (
        gr.update(value="Classify", variant="primary"),
        gr.update(value="", visible=False),
    )


def predict(text):
    if not text or not text.strip():
        return {}
    # truncation=True is a no-op when the text fits and trims to MAX_TOKENS otherwise,
    # so this handles both the normal and the "Truncate & classify" paths.
    scores = pipe(text, truncation=True, max_length=MAX_TOKENS)[0]
    return {row["label"]: float(row["score"]) for row in scores}


# Real 2010s films, one per distinct genre. Plots are the verbatim opening of each
# film's Wikipedia "Plot" section (https://en.wikipedia.org/wiki/2010s_in_film),
# trimmed to stay under the model's 512-token limit. Titles live in example_labels,
# not the input, so they don't bias the classifier.
example_films = [
    ("The Lion King (2019)", """In the Pride Lands of Tanzania, a pride of lions rule over the kingdom from Pride Rock. King Mufasa and Queen Sarabi's newborn son, Simba, is presented to the gathering animals by Rafiki the mandrill, the kingdom's shaman and advisor. Mufasa's younger brother, Scar, covets the throne. Mufasa shows Simba the Pride Lands and forbids him from exploring beyond its borders. He explains to Simba the responsibilities of kingship and the "circle of life", which connects all living things. Scar manipulates Simba into exploring an elephant graveyard beyond the Pride Lands, where Simba and his best friend, Nala, are chased by a clan of spotted hyenas led by the ruthless Shenzi. Mufasa rescues the cubs, and Scar visits the hyenas to convince them to help him kill Mufasa and Simba in exchange for hunting rights in the Pride Lands."""),
    ("Inception", """Dom Cobb and Arthur are "extractors" who perform corporate espionage using experimental dream-sharing technology to infiltrate their targets' subconscious and extract information. Their latest target, Saito, offers to hire Cobb for the ostensibly impossible job of implanting an idea into a person's subconscious; performing "inception" on Robert Fischer, the son of Saito's competitor, with the idea to dissolve his father's company. In return, Saito promises to clear Cobb's criminal status, allowing him to return home to his children. Cobb accepts and assembles his team: a forger named Eames, a chemist named Yusuf, and a college student named Ariadne, who is tasked with designing the dream's architecture."""),
    ("Get Out", """On a suburban street at night, a black man walks alone, talking on the phone. A car pulls beside him; sensing trouble, the man starts to walk away. A person wearing a helmet tackles and subdues the man, drags him into the car and drives away. Chris Washington, a black photographer, travels to upstate New York for a weekend getaway to meet the family of his white girlfriend, Rose Armitage. He has uncomfortable conversations with Rose's parents: Dean, a neurosurgeon, and Missy, a psychiatrist. Later, Rose's brother Jeremy arrives, and Chris observes eerie behavior from the family's black servants, Georgina and Walter."""),
    ("La La Land", """While stuck in Los Angeles traffic, Sebastian "Seb" Wilder has a moment of road rage directed at aspiring actress Amelia "Mia" Dolan. After a hard day at work, Mia's next audition goes awry because the casting director takes a phone call during an emotional scene. During a gig at a restaurant, Seb slips into jazz improvisation despite the owner's warning to only play traditional pieces. Mia hears him playing as she passes by, and observes Seb being fired for his disobedience. Months later, she runs into Seb at a party where he plays in a 1980s pop cover band, and despite their clear chemistry they lament wasting the night on each other."""),
    ("Mad Max: Fury Road", """Max Rockatansky is captured by hydraulic despot cult leader Immortan Joe's War Boys and taken to his fortress called the Citadel. Max, a universal donor, is forced to transfuse his blood to Nux, a sick War Boy. Meanwhile, Joe sends his lieutenant Imperator Furiosa in the armoured "War Rig" to trade produce and water for petrol and ammunition. When Joe realises his five "wives" are fleeing in the Rig, he leads his entire army in pursuit. Nux joins the pursuit with Max strapped to his car, and a chasing battle ensues across the desert wasteland."""),
]
examples = [[plot] for _, plot in example_films]
example_labels = [title for title, _ in example_films]

with gr.Blocks(title="IMDb Plot → Genre Classifier") as demo:
    gr.Markdown(
        f"# IMDb Plot → Genre Classifier\n"
        f"Multi-label genre prediction from plot text. "
        f"Model: [`{MODEL}`](https://huggingface.co/{MODEL})."
    )
    plot = gr.Textbox(lines=6, label="Plot summary", placeholder="Paste a movie or show plot...")
    submit = gr.Button("Classify", variant="primary")
    notice = gr.Markdown(visible=False)
    genres = gr.Label(num_top_classes=5, label="Predicted genres")

    # run_on_click classifies the moment an example is picked, so selecting a film
    # (e.g. "The Lion King") populates the genres without a separate Classify press.
    gr.Examples(
        examples=examples,
        inputs=plot,
        outputs=genres,
        fn=predict,
        run_on_click=True,
        example_labels=example_labels,
    )

    plot.change(on_change, inputs=plot, outputs=[submit, notice])
    submit.click(predict, inputs=plot, outputs=genres)

demo.launch()
