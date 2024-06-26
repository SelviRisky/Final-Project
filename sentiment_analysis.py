from textblob import TextBlob

class SentimentAnalyzer:
    @staticmethod
    def analyze(text):
        blob = TextBlob(text)
        return blob.sentiment.polarity