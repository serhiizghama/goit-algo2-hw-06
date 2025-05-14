from collections import defaultdict
import matplotlib.pyplot as plt
import requests


def get_text(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        text = response.text
    except requests.RequestException as e:
        print(f"Помилка при завантаженні тексту: {e}")
        text = ""

    return text


def map_function(text):
    words = text.split()
    return [(word, 1) for word in words]


def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(shuffled_values):
    reduced = {}
    for key, values in shuffled_values:
        reduced[key] = sum(values)
    return reduced


# Виконання MapReduce
def map_reduce(text):
    # Крок 1: Мапінг
    mapped_values = map_function(text)

    # Крок 2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Крок 3: Редукція
    reduced_values = reduce_function(shuffled_values)

    return reduced_values


# Візуалізація


def visualize_top_words(result, frequency):
    sorted_res = sorted(result.items(), key=lambda x: x[1], reverse=True)[
        :frequency]
    words, counts = zip(*sorted_res)

    plt.figure(figsize=(12, 6))
    plt.barh(words, counts, color="skyblue")
    plt.title(f"Top {frequency} Most Frequent Words")
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.tight_layout()
    plt.gca().invert_yaxis()
    plt.show()


if __name__ == "__main__":
    # Вхідний текст для обробки
    url = "https://www.gutenberg.org/cache/epub/76086/pg76086.txt"
    text = get_text(url)

    # Виконання MapReduce на вхідному тексті
    result = map_reduce(text)

    visualize_top_words(result, 10)
