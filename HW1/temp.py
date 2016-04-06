__author__ = 'snehagaikwad'
import re

if __name__ == "__main__":
    line = "Cats are smarter than dogs .Lsokn"
    matchObj = re.match(r'\w+(\.?\w+)*', line, re.M|re.I)

    if matchObj:
       print "matchObj.group() : ", matchObj.group()
       print "matchObj.group(1) : ", matchObj.group(1)
       #print "matchObj.group(2) : ", matchObj.group(2)
    else:
        print "No match!!"
def getSpan(word_pos_map,query_word_count):
    matrix = []
    for word in word_pos_map.keys():
        pos_list = word_pos_map.setdefault(word,[])
        pos_list_sorted = sorted(pos_list)
        matrix.append(pos_list_sorted)
    matrix_len = matrix.__len__()
    #best_span = query_word_count
    min_span =  sys.maxint
    span_list = []
    row_column_index_map = {}
    j = 0
    for i in range (0,matrix_len,1):
        span_list = span_list + [int(matrix[i][j])]
        row_column_index_map[i] = j
    while span_list.__len__() == matrix_len:
        span, index = calculate_span(span_list)
        #if span in range  (best_span - 2, best_span,1):
         #   min_span =  span
          #  break;
        if span < min_span:
            min_span = span
        j = row_column_index_map[index] + 1
        if j > matrix[index].__len__() - 1:
            break;
        span_list[index] = int(matrix[index][j])
        row_column_index_map[index] = j
    #if min_span < (best_span - 3):
     #   min_span = best_span + 5
    return min_span