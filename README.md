## File Explanation 
buildDictionary.py - The code to generate dictionary 
buildInvertedIndex.py - THe code to generate inverted index
dictionary.csv - Generated dictionary
invertedIndex/part-00000 - Generated inverted index
Solution.ipynb - Notebook which you can run code on

## How to run the code
1. Install docker
2. Intall jupyter/pyspark
```bash
docker pull jupyter/pyspark-notebook
```
3. Run command to start container
```bash
docker run -it --rm -p 8888:8888 -v {path_to_project}:/home/jovyan/work jupyter/pyspark-notebook
```
4. Copy the **buildDictionary.py** to one cell and execute it, it will generate **dictionary.csv**
5. Copy the **buildInvertedIndex.py** to one cell and execute it, it will generate **invertedIndex** folder and put inverted index in **part-00000**
