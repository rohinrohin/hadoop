import random
def write_file(file_ptr, src,  dst):

    file_ptr.write(src+' 1 ')
    for pt in dst:
        file_ptr.write(pt+' ')

    file_ptr.write('\n')

if __name__ == '__main__':

    in_file = open('./Input/small.txt', 'r')
    out_file = open('./Input/modi_edges_small.txt', 'w')
    dst_list = []
    srcs = []
    for line in in_file:
        src ,dst = list(line.strip().split())        
        srcs.append(src)
        if (src == dst):
                continue
        dst_list.append(dst)
        for conseq_line in in_file:
            #nos = random.randint(0,3)
            #if nos != 2:
                #continue
            src_1 , dst1 = list( conseq_line.strip().split())
            if (src_1 == dst1):
                    continue

            if( src_1 != src):             
                write_file(out_file, src, dst_list)
                dst_list = []
                src = src_1
                srcs.append(src)
                dst_list.append(dst1)
                
            else:
                dst_list.append(dst1)


    write_file(out_file, src, dst_list)
    
    in_file.close()
    
    srcs = set(srcs)
   
    in_file = open('./Input/small_vertices.txt', 'r')
    
    for line in in_file:
        if(line.split()[0] not in srcs):
            out_file.write(line.split()[0]+' 1\n')
    in_file.close()

    
    out_file.close()


