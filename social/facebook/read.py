import percolation as P
c = P.check


def readGDF(filename="../data/RenatoFabbri06022014.gdf"):
    """Made to work with gdf files from my own network and friends and groups"""
    with open(filename, "r") as f:
        data = f.read()
    lines = data.split("\n")
    columns = lines[0].split(">")[1].split(",")
    column_names = [i.split(" ")[0] for i in columns]
    data_friends = {cn: [] for cn in column_names}
    for line in lines[1:]:
        if not line:
            break
        if ">" in line:
            columns = line.split(">")[1].split(",")
            column_names2 = [i.split(" ")[0] for i in columns]
            data_friendships = {cn: [] for cn in column_names2}
            continue
        fields = line.split(",")
        if "column_names2" not in locals():
            for i, field in enumerate(fields):
                if column_names[i] in ("name", "groupid"):
                    pass
                elif field.isdigit():
                    field = int(field)
                data_friends[column_names[i]].append(field)
        else:
            for i, field in enumerate(fields):
                if column_names2[i] == "name":
                    pass
                elif field.isdigit():
                    field = int(field)
                data_friendships[column_names2[i]].append(field)
    return {"relations": data_friendships,
            "individuals": data_friends}
