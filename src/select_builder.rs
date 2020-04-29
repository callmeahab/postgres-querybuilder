use crate::bucket::Bucket;
use crate::prelude::*;
use postgres_types::ToSql;

pub struct SelectBuilder {
  columns: Vec<String>,
  from_table: String,
  conditions: Vec<String>,
  joins: Vec<Join>,
  groups: Vec<String>,
  order: Vec<Order>,
  limit: Option<String>,
  offset: Option<String>,
  params: Bucket,
}

impl SelectBuilder {
  /// Create a new select query for a given table
  ///
  /// # Examples
  ///
  /// ```
  /// use postgres_querybuilder::SelectBuilder;
  ///
  /// let mut builder = SelectBuilder::new("users");
  /// ```
  pub fn new(from: &str) -> Self {
    SelectBuilder {
      columns: vec![],
      from_table: from.into(),
      conditions: vec![],
      joins: vec![],
      groups: vec![],
      order: vec![],
      limit: None,
      offset: None,
      params: Bucket::new(),
    }
  }

  /// Add a column to select
  ///
  /// # Examples
  ///
  /// ```
  /// use postgres_querybuilder::SelectBuilder;
  /// use postgres_querybuilder::prelude::QueryBuilder;
  ///
  /// let mut builder = SelectBuilder::new("users");
  /// builder.select("id");
  /// builder.select("email");
  ///
  /// assert_eq!(builder.get_query(), "SELECT id, email FROM users");
  /// ```
  pub fn select(&mut self, column: &str) {
    self.columns.push(column.to_string());
  }

  /// Add a raw where condition
  ///
  /// # Examples
  ///
  /// ```
  /// use postgres_querybuilder::SelectBuilder;
  /// use postgres_querybuilder::prelude::QueryBuilder;
  ///
  /// let mut builder = SelectBuilder::new("users");
  /// builder.add_where_raw("something IS NULL".into());
  ///
  /// assert_eq!(builder.get_query(), "SELECT * FROM users WHERE something IS NULL");
  /// ```
  pub fn add_where_raw(&mut self, raw: String) {
    self.conditions.push(raw);
  }
}

impl SelectBuilder {
  fn select_to_query(&self) -> String {
    let columns = if self.columns.len() == 0 {
      "*".to_string()
    } else {
      self.columns.join(", ")
    };
    format!("SELECT {}", columns)
  }

  fn from_to_query(&self) -> String {
    format!("FROM {}", self.from_table)
  }

  fn where_to_query(&self) -> Option<String> {
    if self.conditions.len() > 0 {
      let result = self.conditions.join(" AND ");
      Some(format!("WHERE {}", result))
    } else {
      None
    }
  }

  fn group_by_to_query(&self) -> Option<String> {
    if self.groups.len() > 0 {
      let result = self.groups.join(", ");
      Some(format!("GROUP BY {}", result))
    } else {
      None
    }
  }

  fn order_by_to_query(&self) -> Option<String> {
    if self.order.len() > 0 {
      let result: Vec<String> = self.order.iter().map(|order| order.to_string()).collect();
      Some(format!("ORDER BY {}", result.join(", ")))
    } else {
      None
    }
  }

  fn limit_to_query(&self) -> Option<String> {
    match self.limit.as_ref() {
      Some(limit) => Some(format!("LIMIT {}", limit)),
      None => None,
    }
  }

  fn offset_to_query(&self) -> Option<String> {
    match self.offset.as_ref() {
      Some(offset) => Some(format!("OFFSET {}", offset)),
      None => None,
    }
  }
}

impl QueryBuilder for SelectBuilder {
  fn add_param<T: 'static + ToSql + Sync + Clone>(&mut self, value: T) -> usize {
    self.params.push(value)
  }

  fn get_query(&self) -> String {
    let mut sections: Vec<String> = vec![];
    sections.push(self.select_to_query());
    sections.push(self.from_to_query());
    match self.where_to_query() {
      Some(value) => sections.push(value),
      None => (),
    };
    match self.group_by_to_query() {
      Some(value) => sections.push(value),
      None => (),
    };
    match self.order_by_to_query() {
      Some(value) => sections.push(value),
      None => (),
    };
    match self.limit_to_query() {
      Some(value) => sections.push(value),
      None => (),
    };
    match self.offset_to_query() {
      Some(value) => sections.push(value),
      None => (),
    };
    sections.join(" ")
  }

  fn get_ref_params(self) -> Vec<&'static (dyn ToSql + Sync)> {
    self.params.get_refs()
  }
}

impl QueryBuilderWithWhere for SelectBuilder {
  fn where_condition(&mut self, raw: &str) {
    self.conditions.push(raw.to_string());
  }
}

impl QueryBuilderWithLimit for SelectBuilder {
  fn limit(&mut self, limit: i64) {
    let index = self.params.push(limit);
    self.limit = Some(format!("${}", index));
  }
}

impl QueryBuilderWithOffset for SelectBuilder {
  fn offset(&mut self, offset: i64) {
    let index = self.params.push(offset);
    self.offset = Some(format!("${}", index));
  }
}

impl QueryBuilderWithJoin for SelectBuilder {
  fn inner_join(&mut self, table_name: &str, relation: &str) {
    self
      .joins
      .push(Join::Inner(table_name.to_string(), relation.to_string()));
  }

  fn left_join(&mut self, table_name: &str, relation: &str) {
    self.joins.push(Join::LeftOuter(
      table_name.to_string(),
      relation.to_string(),
    ));
  }

  fn left_outer_join(&mut self, table_name: &str, relation: &str) {
    self
      .joins
      .push(Join::Left(table_name.to_string(), relation.to_string()));
  }
}

impl QueryBuilderWithGroupBy for SelectBuilder {
  fn group_by(&mut self, field: &str) {
    self.groups.push(field.to_string());
  }
}

impl QueryBuilderWithOrder for SelectBuilder {
  /// Add order attribute to request
  ///
  /// # Examples
  ///
  /// ```
  /// use postgres_querybuilder::SelectBuilder;
  /// use postgres_querybuilder::prelude::Order;
  /// use postgres_querybuilder::prelude::QueryBuilder;
  /// use postgres_querybuilder::prelude::QueryBuilderWithOrder;
  ///
  /// let mut builder = SelectBuilder::new("users");
  /// builder.order_by(Order::Asc("name".into()));
  ///
  /// assert_eq!(builder.get_query(), "SELECT * FROM users ORDER BY name ASC");
  /// ```
  fn order_by(&mut self, field: Order) {
    self.order.push(field);
  }
}

#[cfg(test)]
pub mod test {
  use super::*;

  #[test]
  fn from_scratch() {
    let builder = SelectBuilder::new("publishers");
    assert_eq!(builder.get_query(), "SELECT * FROM publishers");
  }

  #[test]
  fn with_columns() {
    let mut builder = SelectBuilder::new("publishers");
    builder.select("id");
    builder.select("name");
    assert_eq!(builder.get_query(), "SELECT id, name FROM publishers");
  }

  #[test]
  fn with_limit() {
    let mut builder = SelectBuilder::new("publishers");
    builder.select("id");
    builder.limit(10);
    assert_eq!(builder.get_query(), "SELECT id FROM publishers LIMIT $1");
  }

  #[test]
  fn with_limit_offset() {
    let mut builder = SelectBuilder::new("publishers");
    builder.select("id");
    builder.limit(10);
    builder.offset(5);
    assert_eq!(
      builder.get_query(),
      "SELECT id FROM publishers LIMIT $1 OFFSET $2"
    );
  }

  #[test]
  fn with_where_eq() {
    let mut builder = SelectBuilder::new("publishers");
    builder.select("id");
    builder.select("name");
    builder.where_eq("trololo", 42);
    builder.where_eq("tralala", true);
    builder.where_ne("trululu", "trololo");
    assert_eq!(
      builder.get_query(),
      "SELECT id, name FROM publishers WHERE trololo = $1 AND tralala = $2 AND trululu <> $3"
    );
  }

  #[test]
  fn with_order() {
    let mut builder = SelectBuilder::new("publishers");
    builder.select("id");
    builder.order_by(Order::Asc("id".into()));
    builder.order_by(Order::Desc("name".into()));
    assert_eq!(
      builder.get_query(),
      "SELECT id FROM publishers ORDER BY id ASC, name DESC"
    );
  }
}
