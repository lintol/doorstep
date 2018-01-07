import docker

def run_in_sandbox(image, command, args, data_dir):
    client = docker.from_env()
    output = client.containers.run(
        image,
        ' '.join([command] + args),
        cap_drop=['ALL'],
        network_mode='none',
        volumes={
            data_dir: {'bind': '/pfs', 'mode': 'rw'}
        }
    )
    return output
