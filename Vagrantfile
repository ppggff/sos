# Basic Vagrant config (API version 2)
Vagrant.configure(2) do |config|
  config.vm.box = "devel-devtoolset-10-vagrant-virtualbox-centos7-2009"
  config.vm.box_url = "http://artifactory.hashdata.xyz/artifactory/api/vagrant/vagrant-public/devel-devtoolset-10-vagrant-virtualbox-centos7-2009"
  config.vm.box_check_update = true

  config.vm.hostname = "devel"

  # Give a reasonable amount of cpu and memory to the VM
  config.vm.provider "virtualbox" do |vb|
    vb.cpus = 2
    vb.memory = 8192
    vb.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-threshold", "1000"]
    vb.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-start"]
    vb.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-on-restore", "1"]
  end

  # Make the GPDB code folder will be visible as /gpdb in the virtual machine
  config.vm.synced_folder ".", "/code"

  config.vm.provision "main", type: "shell", privileged: false, inline: <<-SHELL
  SHELL
end
