use crate::prelude::*;
use crate::bucket::Bucket;
use postgres_types::ToSql;

pub struct DeleteBuilder {
    table: String,
    conditions: Vec<String>,
    params: Bucket,
}

impl DeleteBuilder {
    /// Create a new delete builder for a given table
    ///
    /// # Examples
    ///
    /// ```
    /// use postgres_querybuilder::DeleteBuilder;
    /// use postgres_querybuilder::prelude::{QueryBuilder, QueryBuilderWithWhere};
    ///
    /// let mut builder = DeleteBuilder::new("users");
    /// builder.where_eq("id", 42);
    ///
    /// assert_eq!(builder.get_query(), "DELETE FROM users WHERE id = $1")
    /// ```
    pub fn new(from: &str) -> DeleteBuilder {
        DeleteBuilder {
            table: from.to_string(),
            conditions: vec![],
            params: Bucket::new(),
        }
    }
}

impl DeleteBuilder {
    fn table_to_query(&self) -> String {
        format!("DELETE FROM {}", self.table)
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

impl QueryBuilder for DeleteBuilder {
    fn add_param<T: 'static + ToSql + Sync + Clone>(&mut self, value: T) -> usize {
        self.params.push(value)
    }

    fn get_query(&self) -> String {
        let mut result: Vec<String> = vec![];
        result.push(self.table_to_query());
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

impl QueryBuilderWithWhere for DeleteBuilder {
    fn where_condition(&mut self, raw: &str) -> &mut Self {
        self.conditions.push(raw.to_string());
        self
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn from_scratch() {
        let builder = DeleteBuilder::new("publishers");
        assert_eq!(builder.get_query(), "DELETE FROM publishers");
    }

    #[test]
    fn with_where() {
        let mut builder = DeleteBuilder::new("publishers");
        builder.where_eq("id", 22);
        assert_eq!(
            builder.get_query(),
            "DELETE FROM publishers WHERE id = $1",
        );
    }
}
