use crate::bucket::Bucket;
use crate::prelude::*;
use postgres_types::ToSql;

pub struct InsertBuilder {
    with_queries: Vec<(String, String)>,
    table: String,
    fields: Vec<String>,
    values: Vec<String>,
    returning_fields: Vec<String>,
    conditions: Vec<String>,
    params: Bucket,
}

impl InsertBuilder {
    /// Create a new insert builder for a given table
    ///
    /// # Examples
    ///
    /// ```
    /// use postgres_querybuilder::InsertBuilder;
    /// use postgres_querybuilder::prelude::{QueryBuilder, QueryBuilderWithValues, QueryBuilderWithWhere, QueryWithField};
    ///
    /// let mut builder = InsertBuilder::new("users");
    /// builder.field("username");
    /// builder.value("rick".to_string());
    ///
    /// assert_eq!(builder.get_query(), "INSERT INTO users (username) VALUES ($1)");
    /// ```
    pub fn new(from: &str) -> Self {
        InsertBuilder {
            with_queries: vec![],
            table: from.into(),
            fields: vec![],
            values: vec![],
            returning_fields: vec![],
            conditions: vec![],
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

    fn returning_fields_to_query(&self) -> Option<String> {
        if self.returning_fields.len() > 0 {
            let returning_query = self.returning_fields.join(", ");
            Some(format!("RETURNING {}", returning_query))
        } else {
            None
        }
    }

    fn where_to_query(&self) -> Option<String> {
        if self.conditions.len() > 0 {
            let where_query = self.conditions.join(" AND ");
            Some(format!("WHERE {}", where_query))
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
        match self.returning_fields_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        match self.where_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        result.join(" ")
    }

    fn get_ref_params(self) -> Vec<&'static (dyn ToSql + Sync)> {
        self.params.get_refs()
    }
}

impl QueryWithField for InsertBuilder {
    fn field(&mut self, field: &str) -> &mut Self {
        self.fields.push(field.to_string());
        self
    }
}

impl QueryWithFields for InsertBuilder {
    fn fields(&mut self, fields: Vec<&str>) -> &mut Self {
        for field in fields {
            self.fields.push(field.to_string());
        }
        self
    }
}

impl QueryBuilderWithWhere for InsertBuilder {
    fn where_condition(&mut self, raw: &str) -> &mut Self {
        self.conditions.push(raw.to_string());
        self
    }
}

impl QueryBuilderWithValues for InsertBuilder {
    fn value<T: 'static + ToSql + Sync + Clone>(&mut self, value: T) -> &mut Self {
        let index = self.params.push(value);
        self.values.push(format!("${}", index));
        self
    }

    // fn values<T: 'static + ToSql + Sync + Clone>(&mut self, values: Vec<T>) -> &mut Self {
    //     for value in values {
    //         value(value);
    //     }
    //     self
    // }
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
        let mut builder = InsertBuilder::new("publishers");
        builder.field("id");
        builder.value(5);
        assert_eq!(
            builder.get_query(),
            "INSERT INTO publishers (id) VALUES ($1)"
        );
    }
}
