def removeSpaces():
    with open('Results.txt', 'r') as file :
        filedata = file.read()
    filedata = filedata.replace('  ', '\t')

    with open('./Data/title.actors.tsv', 'x') as file:
        file.write(filedata)
    
if __name__ == "__main__":
    removeSpaces()