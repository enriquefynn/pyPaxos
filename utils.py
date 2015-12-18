# Returns a dictionary {name: [params]}
def read_config(file_path):
    config = {'number_acceptors': '3', 'timeout_msgs': '100'}
    with open(file_path) as f:
        for line in f:
            parsed_line = line.split()
            config[parsed_line[0]] = parsed_line[1:]
    return config
