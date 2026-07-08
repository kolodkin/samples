IMDb Plot → Genre Classifier (DeBERTa-v3)
---

Fine-tunes `microsoft/deberta-v3-base` (~184M params, up from the earlier `distilbert-base-uncased` fine-tune's 66M) on plot summaries from [`kolodkin/imdb-wikipedia-enriched`](https://huggingface.co/datasets/kolodkin/imdb-wikipedia-enriched) for multi-label genre classification over the genres with enough training data (rare genres and the catch-all "Other" are dropped), with a stratified 80/10/10 split, deduped on plot hash and seeded for reproducibility. The trained model and an auto-generated model card are pushed to [`kolodkin/imdb-genre-deberta-v3`](https://huggingface.co/kolodkin/imdb-genre-deberta-v3); the interactive demo in the [Hugging Face Space](https://huggingface.co/spaces/kolodkin/imdb-genre-distilbert) still serves the earlier DistilBERT model. Runs end-to-end on a Colab free T4 in ~45–60 minutes.

```bash
# Open the notebook in Colab:
# https://colab.research.google.com/github/kolodkin/samples/blob/main/imdb-genre-distilbert/notebook.ipynb

# Or run locally with a GPU:
jupyter lab imdb-genre-distilbert/notebook.ipynb
```
