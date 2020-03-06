from random import randint

def generate_json_txt(limit, filepath):
    NAMES = ["aaa","bbb","ccc"]
    batch = limit / 100
    f = open(filepath, "w")
    for idx in range(limit):
        if idx % batch == 0 and idx > 0:
            print("%s rows generated" % (idx))
        row = '''{"id":%s,"name":"%s"}''' % (idx, NAMES[randint(0, len(NAMES)-1)])
        f.write("%s\n" % (row))
    f.close()

if __name__ == '__main__':
    generate_json_txt(10, "./students-10.json.txt")
    generate_json_txt(100, "./students-100.json.txt")
    generate_json_txt(1000, "./students-1000.json.txt")
    generate_json_txt(10000, "./students-10000.json.txt")
    generate_json_txt(100000, "./students-100000.json.txt")
    generate_json_txt(1000000, "./students-1000000.json.txt")
