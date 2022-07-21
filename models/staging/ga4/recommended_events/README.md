# Recommended Events

The events in this folder are the [GA4 recommended events] (https://support.google.com/analytics/answer/9267735?hl=en).

These events are disabled by default so as not to slow down the building of your models unneccessarily.

To enable these models, enter the event file name, without the file extension, in your `dbt_project.yml` and set the enabled configuration to true.

This is how you would enable the purchase event:

```
models:
  ga4:
    staging:
      ga4:
        recommended_events:
          stg_ga4__event_purchase:
            +enabled: true
```

This is how you would enable all recommended events:

```
models:
  ga4:
    staging:
      ga4:
        recommended_events:
            +enabled: true
```

At the time of writing, only the ecommerce events are currently configured. If you need a specific model, please consider creating a pull request with the model that you need in the [dbt-ga4 GitHub repository](https://github.com/Velir/dbt-ga4).