use crate::bucket::Bucket;
use crate::prelude::*;
use postgres_types::ToSql;

pub struct UpdateBuilder {
    with_queries: Vec<(String, String)>,
    table: String,
    fields: Vec<String>,
    returning_fields: Vec<String>,
    from_items: Vec<String>,
    conditions: Vec<String>,
    params: Bucket,
}

impl UpdateBuilder {
    /// Create a new update builder for a given table
    ///
    /// # Examples
    ///
    /// ```
    /// use postgres_querybuilder::UpdateBuilder;
    /// use postgres_querybuilder::prelude::{QueryBuilder, QueryBuilderWithSet, QueryBuilderWithWhere};
    ///
    /// let user_password = "password".to_string();
    /// let mut builder = UpdateBuilder::new("users");
    /// builder.set("username", "rick".to_string());
    /// builder.where_eq("id", 42);
    ///
    /// assert_eq!(builder.get_query(), "UPDATE users SET username = $1 WHERE id = $2");
    /// ```
    pub fn new(from: &str) -> Self {
        UpdateBuilder {
            with_queries: vec![],
            table: from.into(),
            fields: vec![],
            from_items: vec![],
            returning_fields: vec![],
            conditions: vec![],
            params: Bucket::new(),
        }
    }

    pub fn get_values(&mut self) -> &Vec<Box<(dyn ToSql + Sync + 'static)>> {
        &self.params.content
    }
}

impl UpdateBuilder {
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

    fn table_to_query(&self) -> String {
        format!("UPDATE {}", self.table)
    }

    fn set_to_query(&self) -> Option<String> {
        if self.fields.len() > 0 {
            let fields_query = self.fields.join(", ");
            Some(format!("SET {}", fields_query))
        } else {
            None
        }
    }

    fn from_items_to_query(&self) -> Option<String> {
        if self.from_items.len() > 0 {
            let from_items_query = self.from_items.join(", ");
            Some(format!("FROM {}", from_items_query))
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

impl QueryBuilder for UpdateBuilder {
    fn add_param<T: 'static + ToSql + Sync + Clone>(&mut self, value: T) -> usize {
        self.params.push(value)
    }

    fn get_query(&self) -> String {
        let mut result: Vec<String> = vec![];
        match self.with_queries_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        result.push(self.table_to_query());
        match self.set_to_query() {
            Some(value) => result.push(value),
            None => (),
        };
        match self.from_items_to_query() {
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

impl QueryBuilderWithWhere for UpdateBuilder {
    fn where_condition(&mut self, raw: &str) -> &mut Self {
        self.conditions.push(raw.to_string());
        self
    }
}

impl QueryBuilderWithSet for UpdateBuilder {
    fn set<T: 'static + ToSql + Sync + Clone>(&mut self, field: &str, value: T) -> &mut Self {
        let index = self.params.push(value);
        self.fields.push(format!("{} = ${}", field, index));
        self
    }

    fn set_computed(&mut self, field: &str, value: &str) -> &mut Self {
        self.fields.push(format!("{} = {}", field, value));
        self
    }
}

impl QueryBuilderWithQueries for UpdateBuilder {
    fn with_query(&mut self, name: &str, query: &str) -> &mut Self {
        self.with_queries.push((name.into(), query.into()));
        self
    }
}

impl QueryBuilderWithReturningColumns for UpdateBuilder {
    fn returning(&mut self, fields: Vec<&str>) -> &mut Self {
        for field in fields {
            self.returning_fields.push(field.to_string());
        }
        self
    }
}

impl QueryBuilderWithFrom for UpdateBuilder {
    fn from(&mut self, item: &str) -> &mut Self {
        self.from_items.push(item.into());
        self
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::SelectBuilder;

    #[test]
    fn from_scratch() {
        let builder = UpdateBuilder::new("publishers");
        assert_eq!(builder.get_query(), "UPDATE publishers");
    }

    #[test]
    fn with_fields_and_where() {
        let mut builder = UpdateBuilder::new("publishers");
        builder.where_eq("trololo", 42);
        builder.set("id", 5);
        assert_eq!(
            builder.get_query(),
            "UPDATE publishers SET id = $2 WHERE trololo = $1"
        );
    }

    #[test]
    fn with_computed_fields_and_where() {
        let mut builder = UpdateBuilder::new("publishers");
        builder.where_eq("trololo", 42);
        builder.set("id", 5);
        builder.set_computed("trololo", "md5(42)");
        assert_eq!(
            builder.get_query(),
            "UPDATE publishers SET id = $2, trololo = md5(42) WHERE trololo = $1"
        );
    }

    #[test]
    fn with_set_from_items_and_where() {
        let mut qb = UpdateBuilder::new("features");
        let query = qb
            .where_condition("features.id = tiles.dataset_id")
            .set_computed("geom", "tiles.geom")
            .from("tiles")
            .get_query();
        assert_eq!(
            query,
            "UPDATE features SET geom = tiles.geom FROM tiles WHERE features.id = tiles.dataset_id"
        );
    }

    #[test]
    fn with_set_from_items_where_and_subquery() {
        let mut subquery_builder = SelectBuilder::new("data_delivery_tiles");
        let subquery = subquery_builder
            .select("ST_Transform(ST_Union(data_delivery_tiles.geom), 4674) as geom")
            .where_eq("dataset_id", 0)
            .group_by("dataset_id")
            .get_query();
        let mut builder = UpdateBuilder::new("features");
        let query = builder
            .where_eq("features.id", 1)
            .set_computed("geom", "tiles.geom")
            .from(format!("({}) tiles", subquery).as_str())
            .get_query();
        assert_eq!(
            query.to_lowercase(),
            "update features set geom = tiles.geom from (select st_transform(st_union(data_delivery_tiles.geom), 4674) as geom from data_delivery_tiles where dataset_id = $1 group by dataset_id) tiles where features.id = $1"
        );
    }
}
