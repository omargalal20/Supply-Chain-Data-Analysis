from ReadingDataSet import ReadingDataSet

dataSet = ReadingDataSet()
# Dictionary containing all dataframes
All_dfs = dataSet.All_dfs
print(f"Df Keys: {All_dfs.keys()}")

# import nltk
# nltk.download()

# from nltk.corpus import wordnet

# def get_word_synonyms_from_sent(word, sent):
#     word_synonyms = []
#     for synset in wordnet.synsets(word):
#         for lemma in synset.lemma_names():
#             if lemma in sent and lemma != word:
#                 word_synonyms.append(lemma)
#     return word_synonyms

# word = "happy"
# sent = ['I', 'am', 'glad', 'it', 'was', 'felicitous', '.']
# word_synonyms = get_word_synonyms_from_sent(word, sent)
# print ("WORD:", word)
# print ("SENTENCE:", sent)
# print ("SYNONYMS FOR '" + word.upper() + "' FOUND IN THE SENTENCE: " + ", ".join(word_synonyms))

# ///////////////////////////////

# import spacy

# book1_topics = ['god']
# book2_topics = ['god', 'Christ', 'idol', 'Jesus']

# nlp = spacy.load('en_core_web_md')
# doc1 = nlp(' '.join(book1_topics))
# doc2 = nlp(' '.join(book2_topics))

# print(doc1.similarity(doc2))

# ///////////////////////

# import spacy
  
# nlp = spacy.load('en_core_web_md')
  
# for column in columns:

#     words = f'country {column}'
#     print(f'Words {words}')
    
#     tokens = nlp(words)
    
#     for token in tokens:
#         # Printing the following attributes of each token.
#         # text: the word string, has_vector: if it contains
#         # a vector representation in the model, 
#         # vector_norm: the algebraic norm of the vector,
#         # is_oov: if the word is out of vocabulary.
#         print(token.text, token.has_vector, token.vector_norm, token.is_oov)
    
#     token1, token2 = tokens[0], tokens[1]
    
#     print("Similarity:", token1.similarity(token2))