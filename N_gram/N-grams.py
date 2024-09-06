def generate_ngrams(string, n):
    return [string[i:i+n] for i in range(len(string)-n+1)]

def calculate_similarity(str1, str2, n):
    ngrams1 = set(generate_ngrams(str1.lower(), n))
    ngrams2 = set(generate_ngrams(str2.lower(), n))
    
    shared_ngrams = ngrams1.intersection(ngrams2)
    
    similarity = (2 * len(shared_ngrams)) / (len(ngrams1) + len(ngrams2))
    return similarity * 100  # Return as percentage directly

def main():
    print("N-gram Matching for Master Data Management")
    print("------------------------------------------")
    
    # Get input strings from user
    str1 = input("Enter the first string: ")
    str2 = input("Enter the second string: ")
    
    # Set n-gram size (using bigrams)
    n = 2
    
    # Calculate and display similarity
    similarity_percentage = calculate_similarity(str1, str2, n)
    
    print(f"\nSimilarity between '{str1}' and '{str2}':")
    print(f"Percentage Similarity: {similarity_percentage:.2f}%")
    
    # Determine if it's a potential match
    threshold = 40 # 80% threshold
    if similarity_percentage >= threshold:
        print("\nResult: Potential match found!")
    else:
        print("\nResult: Not a close match.")

if __name__ == "__main__":
    main()