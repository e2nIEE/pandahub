# Variants

A variant is a version of a power network with changes recorded relative to the base network (element addition and
deletion or paramater value changes). Changes to the base network automatically propagate to variants (see caveats below).

All elements have a field "var_type" with possible values "base", "change" and "addition":

* *base*: base-version of the element
* *change*: variant-version of an element with changes compared to base
* *addition*: element which only exists in a variant

Elements with var_type "base" have an additional array field "not_in_var" holding indices of
variants in which the element has been removed or changed.

Elements with var_types "change" and "addition" have an additional field "variant" referencing the variant index for the change/addition.

`PandaHub.get_variant_filter(variant)` can be used to generate a mongodb query filter mixin which restricts query results to a given variant or variants.

Example variant filter for single variant with index 1:

    {"$or": [{"var_type": "base", "not_in_var": {"$ne": 1}},
             {"var_type": {"$in": ["change", "addition"]}, "variant": 1}]}

Filter for base variant:

    {"$or": [{"var_type": {"$exists": False}},
             {"var_type": "base"}]}

## Caveats

### Conflicts

* an element that was changed for a variant is changed in the base variant:
    * field(s) changed in variant are changed to different values
    * field(s) previously identical with variant are changed
* an element in the base variant is changed breaking the variant:
    * net can not be loaded completely - e.g. bus of line added in variant is deleted

### Restrictions
* bulk operations are difficult to implement for variants
