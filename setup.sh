project_path='/home/leonardooliveira/Documentos/github/ifood-nyc-taxi-agency' #change it to your machine path
cd $project_path
export PYTHONPATH=$PATH:$(pwd)

# enable the sh files to be run
chmod 744 *.sh

# install java 11 into the local environment
# this process was made for FEDORA linux distribuiton, here's the doc for linux: https://docs.fedoraproject.org/en-US/quick-docs/installing-java/
sudo dnf install -y java-21-openjdk java-21-openjdk-devel
sudo alternatives --config java # select java-21-openjdk
export JAVA_BIN=$(readlink -f $(which java))
export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
export PATH="$JAVA_HOME/bin:$PATH"
java --version

# verify java installation
if type -p java; then
    echo "Java found in PATH"
else
    echo "Java not found in PATH, please check the installation"
    exit 1
fi

# install terraform into the local environment
# this process was made for FEDORA linux distribuiton, here's the doc for linux: https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager addrepo --from-repofile=https://rpm.releases.hashicorp.com/fedora/hashicorp.repo
sudo dnf -y install terraform
terraform --help

# initialize and apply terraform scripts to create the infraestructure on aws
cd $project_path/terraform
terraform init
terraform plan
terraform apply

# install awscli into the local environment
# this process was made for FEDORA linux distribuiton, here's the doc for linux: https://docs.aws.amazon.com/pt_br/cli/latest/userguide/getting-started-install.html
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version

# configure awscli with the credentials created on terraform
cd $project_path/terraform
AWS_KEY=$(terraform output -json credentials | jq -r '.["terraform-aws"].key')
AWS_SECRET=$(terraform output -json credentials | jq -r '.["terraform-aws"].secret')
aws configure set aws_access_key_id $AWS_KEY
aws configure set aws_secret_access_key $AWS_SECRET
aws configure set default.region us-east-1
aws configure set default.output json
aws sts get-caller-identity


# install pyenv (follow prompts if you don't have build deps)
curl https://pyenv.run | bash

# Then follow pyenv post-install steps to add to your shell (usually in ~/.bashrc)
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

pyenv install 3.11.6
pyenv virtualenv 3.11.6 ifood
pyenv activate ifood

sudo dnf5 install -y bzip2-devel sqlite-devel readline-devel gdbm-devel libdb-devel libuuid-devel tk-devel