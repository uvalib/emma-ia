FROM python:3.8-buster

# update the packages
RUN apt update

# Create the run user and group
RUN addgroup --gid 1000 webservice && adduser -q --uid 1000 --gid 1000 --disabled-login webservice

# set the timezone appropriatly
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
ENV APP_HOME /emma-ia
WORKDIR $APP_HOME

# Create necessary directories
RUN mkdir -p $APP_HOME/scripts
RUN chown -R webservice $APP_HOME && chgrp -R webservice $APP_HOME

# install the application requirements
COPY Pipfile Pipfile.lock $APP_HOME/
RUN pip install pipenv
RUN pipenv install

# install the application
ADD *.py $APP_HOME/

# Update permissions
RUN chown -R webservice $APP_HOME /home/webservice && chgrp -R webservice $APP_HOME /home/webservice

# Define port and startup script
EXPOSE 8080
CMD scripts/entry.sh

# Move in other assets
COPY package/container_bash_profile /home/webservice/.profile
COPY package/scripts/entry.sh $APP_HOME/scripts

# Ensure permissions are correct
RUN chown webservice:webservice /home/webservice/.profile $APP_HOME/scripts/entry.sh && chmod 755 /home/webservice/.profile $APP_HOME/scripts/entry.sh

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
#USER webservice
