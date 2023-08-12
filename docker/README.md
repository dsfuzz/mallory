# Dockerized Jepsen with Vagrant

Note: I (Dylan) made a few changes to '/bin/up' to make it convenient to run locally with docker. This likely needs a few minor adjustments to work now.

This folder contains `Vagrantfile` that spins up a Debian virtual machine which
runs a containerized version of a Jepsen environment, consisting of one
"control node" container and 5 "node" containers which will run your system
under test.

The containers are orchestrated using [Docker
Compose](https://github.com/docker/compose). A script builds a
`docker-compose.yml` file out of fragments in `template/`, because this is the
future, and using `awk` to generate YAML to generate computers is *cloud
native*.

You _can_ run the Docker containers directly on your host machine, but this
requires some finnicky configuration since the containers run `systemd`. If you
would rather not go through that, just use the Vagrant VM.

This Docker image attempts to simplify the setup required by Jepsen.
It is intended to be used by a CI tool or anyone with Docker who wants to try Jepsen themselves.
## Quickstart

### If you want to use the Vagrant VM

Required Vagrant plugins. Before you start, run:

```
vagrant plugin install vagrant-reload
```

Run `vagrant up` (or `vagrant up --provider hyperv`). This will spin up a VM
and configure it.

After this is done, you can log into the VM via:

```
vagrant ssh
cd /jepsen/docker
```

If the shared folder does not mount successfully, try `vagrant up; vagrant
halt; vagrant up; vagrant ssh`. For some reason, the first boot sometimes fails
to mount the shared folder. You might also want to try disabling any VPNs on
the host, which might interfere with the network folder share.

If you are using Hyper-V and are asked for a username and password, you are
supposed to provide your Windows _host_ login details.

### Using Docker

Note, you need docker-compose version >1.25 to allow gateway setting in the docker compose template (these versions merge the v2 and v3 file spec)
Also, don't use package manager version of docker (snap, apt etc.); use docker from the official downloads page.
Lastly, I made some changes to the /bin/up file, and the click github repo is assumed to be at ~/git/click.
Feel free to change as needed.

Assuming you are either using the Vagrant VM or have Docker and docker-compose
already set up on your machine, run (if using the VM, prepend `sudo` to the
commands):

```
bin/up
bin/console
```

... which will drop you into a console on the Jepsen control node.

Your DB nodes are `n1`, `n2`, `n3`, `n4`, and `n5`. You can open as many shells
as you like using `bin/console`. If your test includes a web server (try `lein
run serve` on the control node, in your test directory), you can open it
locally by running using `bin/web`. This can be a handy way to browse test
results.

## Advanced

You can change the number of DB nodes by running (e.g.) `bin/up -n 9`.

If you need to log into a DB node (e.g. to debug a test), you can `ssh n1` (or n2, n3, ...) from inside the control node, or:

```
docker exec -it jepsen-n1 bash
```

(If you are using the VM, run the command as `sudo docker --remote exec -it jepsen-n1 bash`.)

During development, it's convenient to run with `--dev` option, which mounts `$JEPSEN_ROOT` dir as `/jepsen` on Jepsen control container.

Run `./bin/up --help` for more info.
