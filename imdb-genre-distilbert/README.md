IMDb Plot → Genre Classifier (DistilBERT)
---

Fine-tunes `distilbert-base-uncased` on plot summaries from [`kolodkin/imdb-wikipedia-enriched`](https://huggingface.co/datasets/kolodkin/imdb-wikipedia-enriched) for multi-label genre classification over the top 15 genres + "Other", with a stratified 80/10/10 split, deduped on plot hash and seeded for reproducibility. The trained model and an auto-generated model card are pushed to [`kolodkin/imdb-genre-distilbert`](https://huggingface.co/kolodkin/imdb-genre-distilbert), and an interactive demo runs in the [Hugging Face Space](https://huggingface.co/spaces/kolodkin/imdb-genre-distilbert). Runs end-to-end on a Colab free T4 in ~15 minutes.

```bash
# Open the notebook in Colab:
# https://colab.research.google.com/github/kolodkin/samples/blob/main/imdb-genre-distilbert/notebook.ipynb

# Or run locally with a GPU:
jupyter lab imdb-genre-distilbert/notebook.ipynb
```
