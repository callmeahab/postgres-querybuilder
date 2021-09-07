use crate::bucket::Bucket;
use crate::prelude::*;
use postgres_types::ToSql;

pub struct InsertBuilder {
    with_queries: Vec<(String, String)>,
    table: String,
    fields: Vec<String>,
    values: Vec<String>,
    returning_fields: Vec<String>,
    upsert_field: Option<String>,
    upsert_set_fields: Vec<String>,
    params: Bucket,
}

impl InsertBuilder {
    /// Create a new insert builder for a given table
    ///
    /// # Examples
    ///
    /// ```
    /// use postgres_querybuilder::InsertBuilder;
    /// use postgres_querybuilder::prelude::{ QueryBuilder, QueryBuilderWithValues, QueryBuilderWithWhere, QueryWithFields, QueryBuilderWithReturningColumns, QueryBuilderWithOnConflict };
    ///
    /// let mut builder = InsertBuilder::new("users");
    /// builder.fields(vec!["id", "username"]); // pass fields as an array
    /// builder.field("shape"); // pass in a single field
    /// builder.value(22);
    /// builder.value("rick");
    /// builder.value_fragment("ST_Transform(ST_GeomFromGeoJSON(?), ?)", vec![
    ///     "some_geojson", "4263",
    /// ]);
    /// // Alternatively you can pass functions and parameters to wrap value
    /// // builder.value_with_fn("some_geojson", vec!["ST_Transform", "ST_GeomFromGeoJSON"], vec![Some("4263"), None]);
    /// builder.on_conflict("id", vec!["username", "alias"]); // upsert clause
    /// builder.returning(vec!["id"]); // returning columns
    ///
    /// assert_eq!(builder.get_query(), "INSERT INTO users (id, username, shape) VALUES ($1, $2, ST_Transform(ST_GeomFromGeoJSON($3), $4)) ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, alias = EXCLUDED.alias RETURNING id");
    /// ```
    pub fn new(from: &str) -> Self {
        InsertBuilder {
            with_queries: vec![],
            table: from.into(),
            fields: vec![],
            values: vec![],
            returning_fields: vec![],
            upsert_field: None,
            upsert_set_fields: vec![],
            params: Bucket::new(),
        }
    }
}

impl InsertBuilder {
    fn with_queries_to_query(&self) -> Option<String> {
        if self.with_queries.len() > 0 {
            let result: Vec<String> = self
                .with_queries
                .iter()
                .map(|item| format!("{} AS ({})", item.0, item.1))
                .collect();
            Some(format!("WITH {}", result.join(", ")))
        } else {
            None
        }
    }

    fn from_to_query(&self) -> String {
        format!("INSERT INTO {}", self.table)
    }

    fn fields_to_query(&self) -> Option<String> {
        if self.fields.len() > 0 {
            let fields_query = self.fields.join(", ");
            Some(format!("({})", fields_query))
        } else {
            None
        }
    }

    fn values_to_query(&self) -> Option<String> {
        if self.values.len() > 0 {
            let values_query = self.values.join(", ");
            Some(format!("VALUES ({})", values_query))
        } else {
            None
        }
    }

    fn on_conflict_query(&self) -> Option<String> {
        if self.upsert_field.is_some() && self.upsert_set_fields.len() > 0 {
            let upsert_fields = self
                .upsert_set_fields
                .iter()
                .map(|field| format!("{} = EXCLUDED.{}", field, field))
                .collect::<Vec<String>>()
                .join(", ");

            Some(format!(
                "ON CONFLICT ({}) DO UPDATE SET {}",
                self.upsert_field.as_ref().unwrap(),
                upsert_fields
            ))
        } else if self.upsert_field.is_some() {
            Some(format!(
                "ON CONFLICT ({}) DO NOTHING",
                self.upsert_field.as_ref().unwrap()
            ))
        } else {
            None
        }
    }

    fn returning_fields_to_query(&self) -> Option<String> {
        if self.returning_fields.len() > 0 {
            let returning_query = self.returning_fields.join(", ");
            Some(format!("RETURNING {}", returning_query))
        } else {
            None
        }
    }
}

impl QueryBuilder for InsertBuilder {
    fn add_param<T: 'static + ToSql + Sync + Clone>(&mut self, value: T) -> usize {
        self.params.push(value)
    }

    fn get_query(&self) -> String {
        let mut result: Vec<String> = vec![];
        match self.with_queries_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        result.push(self.from_to_query());
        match self.fields_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        match self.values_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        match self.on_conflict_query() {
            Some(value) => result.push(value),
            None => (),
        };
        match self.returning_fields_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        result.join(" ")
    }

    fn get_ref_params(self) -> Vec<&'static (dyn ToSql + Sync)> {
        self.params.get_refs()
    }
}

impl QueryWithFields for InsertBuilder {
    fn field(&mut self, field: &str) -> &mut Self {
        self.fields.push(field.to_string());
        self
    }

    fn fields(&mut self, fields: Vec<&str>) -> &mut Self {
        for field in fields {
            self.fields.push(field.to_string());
        }
        self
    }
}

impl QueryBuilderWithValues for InsertBuilder {
    fn value<T: 'static + ToSql + Sync + Clone>(&mut self, value: T) -> &mut Self {
        let index = self.params.push(value);
        self.values.push(format!("${}", index));
        self
    }

    fn value_fragment<T: 'static + ToSql + Sync + Clone>(&mut self, fragment: &str, values: Vec<T>) -> &mut Self {
        let mut result: String = "".to_owned();
        let mut param_count: usize = 0;
        for character in fragment.chars() {
            if character == '?' {
                let index = if values.get(param_count).is_some() {
                    self.params.push(values.get(param_count).unwrap().clone())
                } else {
                    self.params.push("missing_parameter")
                };
                result.push_str(&format!("${}", index));
                param_count = param_count + 1;
            } else {
                result.push_str(&character.to_string());
            }
        }
        self.values.push(result);
        self
    }

    fn value_with_fn<T: 'static + ToSql + Sync + Clone>(&mut self, value: T, wrapper_fn: Vec<&str>, args: Vec<Option<&str>>) -> &mut Self {
        let index = self.params.push(value);
        let prefix = wrapper_fn.join("(");
        let mut rev_args = args;
        rev_args.reverse();
        let suffix = wrapper_fn.iter().enumerate().map(|(idx, _)| {
            if rev_args.get(idx).is_some() && rev_args.get(idx).unwrap().is_some() {
                format!(", {})", rev_args.get(idx).unwrap().unwrap())
            } else {
                ")".to_string()
            }
        }).collect::<String>();
        self.values.push(format!("{}(${}{}", prefix, index, suffix));
        self
    }
}

impl QueryBuilderWithReturningColumns for InsertBuilder {
    fn returning(&mut self, fields: Vec<&str>) -> &mut Self {
        for field in fields {
            self.returning_fields.push(field.to_string());
        }
        self
    }
}

impl QueryBuilderWithQueries for InsertBuilder {
    fn with_query(&mut self, name: &str, query: &str) -> &mut Self {
        self.with_queries.push((name.into(), query.into()));
        self
    }
}

impl QueryBuilderWithOnConflict for InsertBuilder {
    fn on_conflict(&mut self, conflict_field: &str, update_fields: Vec<&str>) -> &mut Self {
        self.upsert_field = Some(conflict_field.to_string());
        self.upsert_set_fields = update_fields
            .iter()
            .map(|field| field.to_string())
            .collect();
        self
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn from_scratch() {
        let builder = InsertBuilder::new("publishers");
        assert_eq!(builder.get_query(), "INSERT INTO publishers");
    }

    #[test]
    fn with_fields_and_values() {
        let mut builder = InsertBuilder::new("users");
        builder.fields(vec!["id", "username", "shape"]);
        builder.field("alias");
        builder.value(22);
        builder.value("rick");
        builder.value_with_fn("some_geojson", vec!["ST_Transform", "ST_GeomFromGeoJSON"], vec![Some("4362"), None]);
        builder.value("none");
        builder.on_conflict("id", vec!["username", "alias"]);
        builder.returning(vec!["id"]);
        assert_eq!(
            builder.get_query(),
           "INSERT INTO users (id, username, shape, alias) VALUES ($1, $2, ST_Transform(ST_GeomFromGeoJSON($3), 4362), $4) ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, alias = EXCLUDED.alias RETURNING id"
        );
    }
}
