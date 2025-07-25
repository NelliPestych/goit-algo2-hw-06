import string
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter
import requests
import matplotlib.pyplot as plt


def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None

def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word):
    return word.lower(), 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

def map_reduce(text, search_words=None):
    text = remove_punctuation(text)
    words = text.split()

    if search_words:
        words = [word for word in words if word in search_words]

    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    shuffled_values = shuffle_function(mapped_values)

    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

def visualize_top_words(word_counts, top_n=10):
    counter = Counter(word_counts)
    most_common = counter.most_common(top_n)
    words, counts = zip(*most_common)

    plt.figure(figsize=(12, 6))
    plt.barh(words, counts, color='skyblue')
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    url = "https://www.gutenberg.org/files/1342/1342-0.txt"  # Pride and Prejudice

    print("Завантаження тексту...")
    text = get_text(url)
    if text:
        print("Виконання MapReduce...")
        word_counts = map_reduce(text)

        print("Візуалізація результатів...")
        visualize_top_words(word_counts, top_n=10)
    else:
        print("Помилка: Не вдалося завантажити текст.")
