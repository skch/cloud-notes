# Configuring the project

There are three ways to provide the configuration. 

## Pass AWS credentials as parameters

You need AWS credentials to start using the database. You have to pass the following three parameters: key, secret, and region:

```
db = new CloudDatabase();
db.Connect(“aws-key-here”, “was-secret-here”, RegionEndpoint.USEast1);
db.Open(“db-name”);
```

## Use configuration file

You can add the AWS credentials to the App.config or Web.config file:

```
<configuration>
  <appSettings>
    <add key=“aws_key” value=“…” />
    <add key=“aws_secret” value=“…” />
    <add key=“aws_region” value=“us-east-1” />
    <add key=“aws_domain” value=“db-name” />
  </appSettings>
</configuration>
```

In this case you can omit the parameters:

```
db = new CloudDatabase();
db.Connect();
db.Open();
```

Note that you can store AWS credentials in config file, but supply database name as a parameter and vice versa.

