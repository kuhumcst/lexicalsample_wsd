
import argparse
import pandas as pd
from pandas import DataFrame
import glob, os


def read_data(filen, file_id):
    f=open(filen, encoding="utf8")
    lines = [line.rstrip() for line in f]
    f.close()

    analysis_chain = lines[0][0]
    sent=[]
    sentences=[]
    for line in lines[1:]:
        if line=='':
            sentences.append(sent)
            sent=[]
        else:
            sent.append(line)
    if sent != []:
        sentences.append(sent)
    print ("Loaded "+str(len(sentences))+" sentences from "+filen)

    only_data = [[word.split('\t') for word in sentence[2:]]for sentence in sentences]

    unannotated_indices=[]
    sentences_list={}
    for inds,sent in enumerate(only_data):
        if (max([len(feats) for feats in sent])) < 5: #Skip unannotated sentences
            unannotated_indices.append(inds)
        else:
            sent_df = pd.DataFrame(sent, columns=['wid','word','lemma','pos','label'])
            assert (sent_df['wid'][0].split('-')[0] == str(inds+1)) #I assume that the sentence ID marked at the first column is consistent wihin the file
            sentences_list[str(file_id) +"_"+ str(inds)]=sent_df

    if (sentences_list != {}):
        #https://stackoverflow.com/questions/17954520/pandas-dataframe-within-dataframe
        sentences_df = pd.concat(sentences_list.values(), keys=sentences_list)
        print (sentences_df.shape)
        return sentences_df
    print ("There was/were "+str(len(unannotated_indices))+" unannotated sentences")
    return pd.DataFrame()

def read_files_from_directory (direc):
    words = glob.glob(direc+"/*")
    for indword, word in enumerate(words):
        if (os.path.isdir(word)):
            words_dataframe_list=[]
            print (word)
            files = glob.glob(word+"/*.tsv")
            for indfile, file in enumerate(files):
#                print (file)
                each_dataframe = read_data(file, indfile)
                if not each_dataframe.empty:
#                    print (each_dataframe.head())
                    words_dataframe_list.append(each_dataframe)
            words_dataframe = pd.concat(words_dataframe_list)
#            print (words_dataframe.shape)
#            res = words_dataframe.loc['1_2']
#            print (res.columns)
#            print (type(res))
#            print (res.iloc[:])
            for sentence in words_dataframe.loc[:]:
                print (sentence)

            
#            f_w = open ("out/output-file"+str(indword)+".html", "w", encoding="utf8")
#            f_w.write (words_dataframe.to_html())
#            f_w.close()

import re
def read_grouped_files_from_directory (direc, outdirec):
    words = glob.glob(direc+"/*")
    for indword, word in enumerate(words):
#        print (word)
        files = glob.glob(word+"/*.tsv")
#        files_per_word = set([re.sub ("\d+\.tsv$", "", file) for indfile, file in enumerate(files)]) #Old version
        files_per_word = set([re.sub ("\_[a-zA-Z0-9]+\.tsv$", "", file) for indfile, file in enumerate(files)])
#        print ("Files per word")
#        print (files_per_word)
#        print ("Files")
#        print (files)
        whole_words_list=[]
        for indfile, file in enumerate(files_per_word):
#            print ("\t"+file)
            frames=[]
            for indanno, annotation in enumerate(glob.glob(file+"*")):
#                print ("\t\t"+annotation)
#                input()
                df = read_data (annotation, indfile)
#                print (df.shape)
#                f_w = open ("out/input_before_merge_"+str(indword)+"_"+str(indfile)+"_"+str(indanno)+".html","w")
#                f_w.write (df.to_html())
#                f_w.close()
                frames.append(df)
#                input()
#            print (len(frames))
            res_df = frames[0]
            for ind_each_frame, each_frame in enumerate(frames[1:]):
                res_df = our_merge (res_df, each_frame)
#            f_w = open ("out/output"+str(indword)+"_"+str(indfile)+".html","w")
#            f_w.write (res_df.to_html())
#            f_w.close()
#            print ("I wrote!")
            whole_words_list.append(res_df)
        whole_words_df = pd.concat(whole_words_list)
        f_w = open (outdirec+"/output"+str(indword)+".csv","w")
        f_w.write (whole_words_df.to_csv(sep='\t'))
        f_w.close()


#This function will take two dataframes as input.
#Each dataframe will contain the same information tagged by different annotators.
#Then, if there is any difference, it must be in the 'label' column.
#This function will return a dataframe, where the labels of the two dataframes are included, separated by a |
def our_merge (df1, df2):
    if df2.empty:
        return df1
    for ind in df1.index:
#        label1 = df1.loc[ind].label.split("|")
#        label2 = df2.loc[ind].label
#        if label2 not in label1 and label2 != '_':
#            df1.loc[ind].label = "|".join(label1) + "|" + label2
        label1_list = df1.loc[ind].label.split("|")
        label2 = df2.loc[ind].label
        label1_list.append(label2)
        df1.loc[ind].label = "|".join(label1_list)
    return df1

def main(filename, outfilename):
    ##CREATE FILES FOR TRAINING (CREATE ONE INSTANCE FOR EACH ANNOTATION)
#    read_files_from_directory (filename)

    ##CREATE FILES FOR TESTING (IN EACH SENTENCE, WE WILL PUT ALL POSSIBLE ANNOTATIONS (WITHOUT DUPLICATING))
    read_grouped_files_from_directory (filename, outfilename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This program will read the data from a TSV file and return it as a Pandas DataFrame.')
    parser.add_argument('--folder', type=str,
                    help='The folder from which we will read the data (e.g. <data>).')
    parser.add_argument('--outfolder', type=str,
                    help='The folder in which we will save the output data (e.g. <out>). The directory MUST exist.')

    args = parser.parse_args()
    main(args.folder, args.outfolder)
