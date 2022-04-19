#
#
#

FROM public.ecr.aws/docker/library/python:3.8-slim-buster

# update the packages
RUN apt update

ENV APP_HOME=/emma-ia \
    USER=webservice \
    GROUP=webservice \
    TZ=UTC

# Create the run user and group
RUN addgroup --quiet --gid 1000 $GROUP && \
    yes | adduser --quiet --uid 1000 --gid 1000 --disabled-login $USER

# set the timezone appropriatly
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
WORKDIR $APP_HOME

# Create necessary directories
RUN mkdir -p $APP_HOME/scripts $APP_HOME/app

# Move in other assets
COPY package/container_bash_profile /home/$USER/.profile
COPY package/scripts/entry.sh $APP_HOME/scripts/
RUN chmod 755 /home/$USER/.profile $APP_HOME/scripts/entry.sh

# install the application requirements
COPY Pipfile Pipfile.lock $APP_HOME/
RUN pip install pipenv
RUN pipenv install

# install the application
COPY *.py $APP_HOME/
COPY app/ $APP_HOME/app/

# Update permissions
RUN chown -R $USER:$GROUP $APP_HOME /home/$USER

# Add the build tag
ARG BUILD_TAG=0
RUN touch $APP_HOME/buildtag.build-$BUILD_TAG

# Define port and startup script
EXPOSE 8080
CMD scripts/entry.sh

#
# end of file
#
