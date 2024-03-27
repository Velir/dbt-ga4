# Recommended Events

The events in this folder are the [GA4 recommended events] (https://support.google.com/analytics/answer/9267735?hl=en).

These events are disabled by default so as not to slow down the building of your models unneccessarily.

To enable these models, enter the event file name, without the file extension, in your `dbt_project.yml` and set the enabled configuration to true.

This is how you would enable the purchase event.

```
models:
  ga4:
    staging:
      recommended_events:
        stg_ga4__event_purchase:
          +enabled: true
```

This is how you would enable all recommended events:

```
models:
  ga4:
    staging:
      recommended_events:
          +enabled: true
```

Not all recommended events have been implemented. If you need a specific event, please consider creating a pull request with the model that you need in the [dbt-ga4 GitHub repository](https://github.com/Velir/dbt-ga4).

## Purchase Event Transaction Deduplication

The `stg_ga4__event_purchase_deduplicated` model builds on the `sgt_ga4__event_purchase` model. It is disabled by default and thus needs to be enabled along with the `stg_ga4__event_purchase` model.

The model only processes purchase events that fall within the window as defined by `static_incremental_days` and can only reliably be expected to deduplicate purchase events occurring in the same day.

The model provides a highly-performant, minimum-viable product for this feature returning only data from the first purchase event with a matching `transaction_id` within the processing window.

You are encouraged to copy this model to your project and customize it there should this MVP be insufficient for your needs.