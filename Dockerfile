FROM ironmerchant/openmpi:latest

# Install python
RUN yum -y update \
&& yum -y install python3 \
&& yum -y install python3-devel \
&& yum -y install python3-pip

# Set pip alias
RUN echo 'alias pip=pip3' >> /root/.bashrc \
&& source /root/.bashrc

# Install mpi4py
RUN wget https://bitbucket.org/mpi4py/mpi4py/downloads/mpi4py-3.0.3.tar.gz \
&& tar -zxf mpi4py-3.0.3.tar.gz \
&& cd mpi4py-3.0.3 \
&& python3 setup.py build \
&& python3 setup.py install

# Install git and get modin repo
RUN yum -y install git \
&& git clone https://github.com/richardlin047/modin.git
