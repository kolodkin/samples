LinkedIn Post
---

🎬 I built a small language model that reads a movie plot and predicts its genres.

The model is a fine-tuned DistilBERT doing multi-label classification over 21 genres — from Drama and Comedy down to War and Western.

For training data, I combined the official IMDb dataset (~10M titles) with Wikipedia plot texts to build a curated dataset of ~222k movies, each with a plot summary and genre tags.

An honest finding: full fine-tuning beat a simple logistic-regression head on the frozen pretrained encoder by only ~5% — the pretrained features already carry most of the signal. Still, the test results turned out pretty nice: 0.61 micro-F1 across 21 genres, with Documentary, Drama, and Horror the easiest to recognize, and Fantasy and Musical the hardest.

Try it yourself — paste any plot (or make one up) into the live demo:
https://huggingface.co/spaces/kolodkin/imdb-genre-distilbert

Links to the datasets and the training Colab in the comments 👇

#MachineLearning #NLP #HuggingFace #DataScience

Comment
---

🔗 Links:

📊 Curated dataset (IMDb + Wikipedia, ~222k movies): https://huggingface.co/datasets/kolodkin/imdb-wikipedia-enriched

🤖 Model card with per-genre test results: https://huggingface.co/kolodkin/imdb-genre-distilbert

📓 Training Colab (runs end-to-end on a free T4 in ~15 min): https://colab.research.google.com/github/kolodkin/samples/blob/main/imdb-genre-distilbert/notebook.ipynb

🛠️ Dataset-building pipeline and all source code: https://github.com/kolodkin/samples
