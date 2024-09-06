from collections import defaultdict

def generate_ngrams(text, n):
    """Generate n-grams from a given text."""
    text = text.lower()  # Convert to lowercase for uniformity
    return [text[i:i+n] for i in range(len(text)-n+1)]

def calculate_similarity(str1, str2):
    """Calculate similarity between two strings using n-gram matching."""
    n = 2  # Using bigrams
    ngrams1 = generate_ngrams(str1, n)
    ngrams2 = generate_ngrams(str2, n)

    # Count occurrences of each n-gram
    count1 = defaultdict(int)
    count2 = defaultdict(int)

    for gram in ngrams1:
        count1[gram] += 1
    for gram in ngrams2:
        count2[gram] += 1

    # Calculate intersection and union
    intersection = sum((count1[gram] * count2[gram]) for gram in count1 if gram in count2)
    
    # Total unique grams
    total_unique_ngrams = len(set(ngrams1 + ngrams2))

    # Similarity score calculation
    similarity_score = intersection / total_unique_ngrams if total_unique_ngrams > 0 else 0
    
    return similarity_score

# Example usage
# str_a = "Apple Inc."
# str_b = "apple incorporated"
str_a = input("enter the 1st name:")
str_b = input("enter the 2nd name:")


similarity = calculate_similarity(str_a, str_b)
print(f"Similarity Score: {similarity:.2f}")