use std::io;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// Merges two schemas into a new schema.
pub fn paste_schema(s0: SchemaRef, s1: SchemaRef) -> SchemaRef {
    let mut fields = s0.fields().to_vec();
    fields.extend_from_slice(s1.fields());
    std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Pastes two iterators of record batches together.
///
/// If the schemas are not provided, they are inferred from the first batch of each iterator.
///
/// # Arguments
///
/// * `b0` - The first iterator of record batches.
/// * `b1` - The second iterator of record batches.
/// * `os0` - An optional schema for the first iterator.
/// * `os1` - An optional schema for the second iterator.
///
/// # Returns
///
/// An iterator that yields the pasted record batches.
pub fn paste_sync<I, J>(
    mut b0: I,
    mut b1: J,
    os0: Option<SchemaRef>,
    os1: Option<SchemaRef>,
) -> Box<dyn Iterator<Item = Result<RecordBatch, io::Error>> + 'static>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>> + 'static,
    J: Iterator<Item = Result<RecordBatch, io::Error>> + 'static,
{
    let first_rb0 = b0.next();
    let first_rb1 = b1.next();

    let (s0, s1) = match (os0, os1) {
        (Some(s0), Some(s1)) => (s0, s1),
        _ => match (first_rb0.as_ref(), first_rb1.as_ref()) {
            (Some(Ok(rb0)), Some(Ok(rb1))) => (rb0.schema(), rb1.schema()),
            _ => return Box::new(std::iter::empty()),
        },
    };

    let bz = first_rb0.into_iter().chain(b0);
    let bo = first_rb1.into_iter().chain(b1);

    Box::new(paste_sync_alt(bz, bo, s0, s1))
}

/// Pastes two iterators of record batches together, given the schemas.
///
/// # Arguments
///
/// * `b0` - The first iterator of record batches.
/// * `b1` - The second iterator of record batches.
/// * `s0` - The schema for the first iterator.
/// * `s1` - The schema for the second iterator.
///
/// # Returns
///
/// An iterator that yields the pasted record batches.
pub fn paste_sync_alt<I, J>(
    b0: I,
    b1: J,
    s0: SchemaRef,
    s1: SchemaRef,
) -> impl Iterator<Item = Result<RecordBatch, io::Error>>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>> + 'static,
    J: Iterator<Item = Result<RecordBatch, io::Error>> + 'static,
{
    let schema = paste_schema(s0, s1);
    b0.zip(b1).map(move |(rb0, rb1)| {
        let rb0 = rb0?;
        let rb1 = rb1?;
        let mut columns = rb0.columns().to_vec();
        columns.extend_from_slice(rb1.columns());
        RecordBatch::try_new(schema.clone(), columns).map_err(io::Error::other)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_paste_schema() {
        let s0 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let s1 = Arc::new(Schema::new(vec![Field::new("b", DataType::Utf8, true)]));

        let pasted_schema = paste_schema(s0.clone(), s1.clone());

        let expected_fields = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ];
        let expected_schema = Arc::new(Schema::new(expected_fields));

        assert_eq!(pasted_schema.fields().len(), 2);
        assert_eq!(pasted_schema, expected_schema);
    }

    #[test]
    fn test_paste_sync() -> Result<(), Box<dyn std::error::Error>> {
        let s0 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let s1 = Arc::new(Schema::new(vec![Field::new("b", DataType::Utf8, true)]));

        let a = arrow::array::Int64Array::from(vec![1, 2, 3]);
        let b = arrow::array::StringArray::from(vec!["a", "b", "c"]);

        let rb0 = RecordBatch::try_new(s0.clone(), vec![Arc::new(a)])?;
        let rb1 = RecordBatch::try_new(s1.clone(), vec![Arc::new(b)])?;

        let b0 = vec![Ok(rb0)].into_iter();
        let b1 = vec![Ok(rb1)].into_iter();

        let mut pasted = paste_sync(b0, b1, None, None);
        let pasted_rb = pasted.next().ok_or("no next value")??;

        let expected_fields = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ];
        let expected_schema = Arc::new(Schema::new(expected_fields));

        assert_eq!(pasted_rb.schema(), expected_schema);
        assert_eq!(pasted_rb.num_columns(), 2);
        assert_eq!(pasted_rb.num_rows(), 3);
        Ok(())
    }

    #[test]
    fn test_paste_sync_alt() -> Result<(), Box<dyn std::error::Error>> {
        let s0 = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let s1 = Arc::new(Schema::new(vec![Field::new("b", DataType::Utf8, true)]));

        let a = arrow::array::Int64Array::from(vec![1, 2, 3]);
        let b = arrow::array::StringArray::from(vec!["a", "b", "c"]);

        let rb0 = RecordBatch::try_new(s0.clone(), vec![Arc::new(a)])?;
        let rb1 = RecordBatch::try_new(s1.clone(), vec![Arc::new(b)])?;

        let b0 = vec![Ok(rb0)].into_iter();
        let b1 = vec![Ok(rb1)].into_iter();

        let mut pasted = paste_sync_alt(b0, b1, s0, s1);
        let pasted_rb = pasted.next().ok_or("no next value")??;

        let expected_fields = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ];
        let expected_schema = Arc::new(Schema::new(expected_fields));

        assert_eq!(pasted_rb.schema(), expected_schema);
        assert_eq!(pasted_rb.num_columns(), 2);
        assert_eq!(pasted_rb.num_rows(), 3);
        Ok(())
    }
}
