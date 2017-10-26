# NiFi Satori Bundle
Custom NiFi Processors for integrating with Satori RTM (https://www.satori.com/docs/using-satori/overview)
 
# Building from Source
To build, simply clone and build with maven:
```
git clone https://github.com/laurencedaluz/nifi-satori-bundle.git
cd nifi-satori-bundle
mvn clean install
```

# Installation
To install the bundle, you will need to copy the nar file into the ```lib``` directory of your NiFi installation and restart NiFi.

# Processors
The project currently provides the following processors:
 * ```ConsumeSatoriRtm``` - used to consume a live data feed from a Satori RTM channel
