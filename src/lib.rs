use fallible_iterator::FallibleIterator;
use postgres::{
    RowIter,
    row::Row,
    error::Error as PsqlError,
    types::{Type,FromSql,WasNull,ToSql},
    Client, NoTls,
};
use serde::de::{
    self,Visitor,Deserialize,DeserializeSeed,MapAccess,
    IntoDeserializer,
};
use std::{
    error::Error as StdError,
    marker::PhantomData,
};

pub use postgres::Statement;

#[cfg(test)] mod tests;

#[derive(Debug)]
pub enum Error {
    Message(String),
    Connect(PsqlError),
    Select(PsqlError),
    SelectNext(PsqlError),
    TryGet{ rust_type: &'static str, psql_type: String, psql_name: String, error: PsqlError },
    ForbiddenCast{ rust_type: &'static str, psql_type: String, psql_name: String },
    Any,
    IgnoredAny,
    Char,
    Tuple,
    TupleStruct,
    Seq,
    Identifier,
    Enum,
    NewtypeStruct,
    Unit,
    UnitStruct,

    Json(serde_json::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(&format!("{:?}",self))
    }
}
impl std::error::Error for Error {}
impl de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}


struct Deserializer<'de> {
    allow_itou_cast: bool,
    index: usize,
    row: &'de Row,
}
impl<'de> Deserializer<'de> {
    fn from_row(row: &'de Row) -> Self {
        Deserializer { row, index: 0, allow_itou_cast: false }
    }
    fn allow_itou(mut self) -> Self {
        self.allow_itou_cast = true;
        self
    }
}

pub fn from_row<'d, T>(row: &'d Row) -> Result<T, Error>
where
    T: Deserialize<'d>,
{
    let mut deserializer = Deserializer::from_row(row);
    T::deserialize(&mut deserializer)
}
pub fn from_row_allow_itou<'d, T>(row: &'d Row) -> Result<T, Error>
where
    T: Deserialize<'d>,
{
    let mut deserializer = Deserializer::from_row(row).allow_itou();
    T::deserialize(&mut deserializer)
}

impl<'de, 'a, 'b> de::Deserializer<'de> for &'a mut Deserializer<'b> {
    type Error = Error;

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,bool>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "bool",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(i) => visitor.visit_bool(i),
        }
    }
    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,i8>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "i8",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(i) => visitor.visit_i8(i),
        }
    }
    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,i16>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "i16",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(i) => visitor.visit_i16(i),
        }
    }
    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,i32>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "i32",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(i) => visitor.visit_i32(i),
        }
    }
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,i64>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "i64",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(i) => visitor.visit_i64(i),
        }
    }
    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.allow_itou_cast {
            true => match self.row.try_get::<_,i8>(self.index) {
                Err(e) => Err(Error::TryGet{ rust_type: "i8",
                                             psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                             psql_name: self.row.columns()[self.index].name().to_string(),
                                             error: e }),
                Ok(i) => visitor.visit_i8(i),
            },
            false => Err(Error::ForbiddenCast{ rust_type: "u8",
                                               psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                               psql_name: self.row.columns()[self.index].name().to_string() }),
        }
    }
    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.allow_itou_cast {
            true => match self.row.try_get::<_,i16>(self.index) {
                Err(e) => Err(Error::TryGet{ rust_type: "i16",
                                             psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                             psql_name: self.row.columns()[self.index].name().to_string(),
                                             error: e }),
                Ok(i) => visitor.visit_i16(i),
            },
            false => Err(Error::ForbiddenCast{ rust_type: "u16",
                                               psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                               psql_name: self.row.columns()[self.index].name().to_string() }),
        }
    }
    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,u32>(self.index) {
            Ok(u) => visitor.visit_u32(u),
            Err(_) => match self.allow_itou_cast {
                true => match self.row.try_get::<_,i32>(self.index) {
                    Err(e) => Err(Error::TryGet{ rust_type: "i32",
                                                 psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                                 psql_name: self.row.columns()[self.index].name().to_string(),
                                                 error: e }),
                    Ok(i) => visitor.visit_i32(i),
                },
                false => Err(Error::ForbiddenCast{ rust_type: "u32",
                                                   psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                                   psql_name: self.row.columns()[self.index].name().to_string() }),
            },
        }
    }
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.allow_itou_cast {
            true => match self.row.try_get::<_,i64>(self.index) {
                Err(e) => Err(Error::TryGet{ rust_type: "i64",
                                             psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                             psql_name: self.row.columns()[self.index].name().to_string(),
                                             error: e }),
                Ok(i) => visitor.visit_i64(i),
            },
            false => Err(Error::ForbiddenCast{ rust_type: "u64",
                                               psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                               psql_name: self.row.columns()[self.index].name().to_string() }),
        }
    }
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,f32>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "f32",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(f) => visitor.visit_f32(f),
        }
    }
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,f64>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "f64",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(f) => visitor.visit_f64(f),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,&str>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "&str",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(s) => visitor.visit_str(s),
        }
    }
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }
    
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_bytes(visitor)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {        
        match self.row.try_get::<_,&[u8]>(self.index) {
            Err(e) => Err(Error::TryGet{ rust_type: "&[u8]",
                                         psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                         psql_name: self.row.columns()[self.index].name().to_string(),
                                         error: e }),
            Ok(s) => visitor.visit_bytes(s),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.row.try_get::<_,PlaceHolder>(self.index) {
            Ok(_) => visitor.visit_some(self),
            Err(e) => match e.source() {
                Some(e) if e.is::<WasNull>() => visitor.visit_none(),
                _ => visitor.visit_some(self),
            },
        }
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_map(self)
    }
    fn deserialize_struct<V: Visitor<'de>>(self, _name: &'static str, _fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_map(visitor)
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::Char) }
    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::Unit) }
    fn deserialize_unit_struct<V: Visitor<'de>>(self, _name: &'static str, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::UnitStruct) }
    fn deserialize_newtype_struct<V: Visitor<'de>>(self, _name: &'static str, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::NewtypeStruct) }
    fn deserialize_seq<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::Seq) }
    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::Tuple) }
    fn deserialize_tuple_struct<V: Visitor<'de>>(self, _name: &'static str, _len: usize, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::TupleStruct) }   
    fn deserialize_enum<V: Visitor<'de>>(self, _name: &'static str, _variants: &'static [&'static str], _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::Enum) }
    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::Identifier) }
    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::Any) }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> { Err(Error::IgnoredAny) }  
}

#[derive(Debug,Clone,Copy)]
enum TimestampTz {
    SystemTime,
    ChronoUtc,
    ChronoLocal,
    TimeOffset,
}
impl TimestampTz {
    fn cast<T>() -> Option<TimestampTz> {
        let casts = [
            (std::any::type_name::<std::time::SystemTime>(),TimestampTz::SystemTime),
            (std::any::type_name::<chrono::DateTime<chrono::Utc>>(),TimestampTz::ChronoUtc),
            (std::any::type_name::<chrono::DateTime<chrono::Local>>(),TimestampTz::ChronoLocal),
            (std::any::type_name::<time::OffsetDateTime>(),TimestampTz::TimeOffset),
        ];
        let name = std::any::type_name::<T>();
        for (n,t) in casts.iter() {
            if *n == name {
                return Some(*t);
            }
        }
        None
    }
    fn into_deserializer(&self, row: &Row, index: usize) -> Result<serde_json::Value,Error> {
        fn get_value<'t,T: FromSql<'t>>(row: &'t Row, index: usize) -> Result<T,Error> {
            row.try_get::<_,T>(index)
                .map_err(|e| Error::TryGet{ rust_type: std::any::type_name::<T>(),
                                            psql_type: row.columns()[index].type_().name().to_string(),
                                            psql_name: row.columns()[index].name().to_string(),
                                            error: e })
        }

        match self {
            TimestampTz::SystemTime => serde_json::to_value(&get_value::<std::time::SystemTime>(row,index)?),
            TimestampTz::ChronoUtc => serde_json::to_value(&get_value::<chrono::DateTime<chrono::Utc>>(row,index)?),
            TimestampTz::ChronoLocal => serde_json::to_value(&get_value::<chrono::DateTime<chrono::Local>>(row,index)?),
            TimestampTz::TimeOffset => serde_json::to_value(&get_value::<time::OffsetDateTime>(row,index)?),
        }.map_err(Error::Json)
    }
}


#[derive(Debug,Clone,Copy)]
enum Timestamp {
    SystemTime,
    ChronoNaive,
    TimePrimitive,
}
impl Timestamp {
    fn cast<T>() -> Option<Timestamp> {
        let casts = [
            (std::any::type_name::<std::time::SystemTime>(),Timestamp::SystemTime),
            (std::any::type_name::<chrono::NaiveDateTime>(),Timestamp::ChronoNaive),
            (std::any::type_name::<time::PrimitiveDateTime>(),Timestamp::TimePrimitive),
        ];
        let name = std::any::type_name::<T>();
        for (n,t) in casts.iter() {
            if *n == name {
                return Some(*t);
            }
        }
        None
    }
    fn into_deserializer(&self, row: &Row, index: usize) -> Result<serde_json::Value,Error> {
        fn get_value<'t,T: FromSql<'t>>(row: &'t Row, index: usize) -> Result<T,Error> {
            row.try_get::<_,T>(index)
                .map_err(|e| Error::TryGet{ rust_type: std::any::type_name::<T>(),
                                            psql_type: row.columns()[index].type_().name().to_string(),
                                            psql_name: row.columns()[index].name().to_string(),
                                            error: e })
        }

        match self {
            Timestamp::SystemTime => serde_json::to_value(&get_value::<std::time::SystemTime>(row,index)?),
            Timestamp::ChronoNaive => serde_json::to_value(&get_value::<chrono::NaiveDateTime>(row,index)?),
            Timestamp::TimePrimitive => serde_json::to_value(&get_value::<time::PrimitiveDateTime>(row,index)?),
        }.map_err(Error::Json)
    }
}

impl<'t,'de> MapAccess<'t> for Deserializer<'de> {
    type Error = Error;
    
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>,Error>
    where K: DeserializeSeed<'t>
    {
        match self.index < self.row.len() {
            true => seed.deserialize(self.row.columns()[self.index].name().into_deserializer()).map(Some),
            false => Ok(None),
        }
    }
    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value,Error>
    where V: DeserializeSeed<'t>
    {
        /*println!("DEBUG: {}: {} => {}",
                 self.row.columns()[self.index].name(),
                 self.row.columns()[self.index].type_().name(),
                 std::any::type_name::<V::Value>());*/
        let r = match self.row.columns()[self.index].type_() {
            &Type::TIMESTAMP => match Timestamp::cast::<V::Value>() {
                None => Err(Error::ForbiddenCast{ rust_type: std::any::type_name::<V::Value>(),
                                                  psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                                  psql_name: self.row.columns()[self.index].name().to_string() }),
                Some(cast) => seed.deserialize(cast.into_deserializer(self.row,self.index)?).map_err(Error::Json),
            },
            &Type::TIMESTAMPTZ => match TimestampTz::cast::<V::Value>() {
                None => Err(Error::ForbiddenCast{ rust_type: std::any::type_name::<V::Value>(),
                                                  psql_type: self.row.columns()[self.index].type_().name().to_string(),
                                                  psql_name: self.row.columns()[self.index].name().to_string() }),
                Some(cast) => seed.deserialize(cast.into_deserializer(self.row,self.index)?).map_err(Error::Json),
            },
            _ => seed.deserialize(&mut *self),
        };
        self.index += 1;
        r
    }
}

// To deserialize NULL postgres values to Option
struct PlaceHolder;
impl<'a> FromSql<'a> for PlaceHolder {
    fn from_sql(_ty: &Type, _raw: &'a [u8]) -> Result<Self, Box<dyn StdError + 'static + Sync + Send>> {
        Ok(PlaceHolder)
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }
}


pub struct Psql {
    client: Client,
    allow_itou: bool,
}
impl Psql {
    pub fn new(con: &str) -> Result<Psql,Error> {
        Ok(Psql {
            client: Client::connect(con, NoTls).map_err(Error::Connect)?,
            allow_itou: false,
        })
    }
    pub fn allow_itou(mut self) -> Self {
        self.allow_itou = true;
        self
    }
    pub fn query_str<'d,T: Deserialize<'d>>(&'d mut self, query: &str) -> ResultIterator<'d,T> {
        ResultIterator {
            allow_itou: self.allow_itou,
            iter: Some(self.client.query_raw(query, std::iter::empty()).map_err(Error::Select)),
            _t: PhantomData,
        }
    }
    pub fn query<'s,'d, Q: PsqlDeserialize<'s,'d>>(&'d mut self, query: &'s Q) -> ResultIterator<'d,Q::Item> {
        ResultIterator {
            allow_itou: self.allow_itou,
            iter: Some(self.client.query_raw(query.query(),query.params()).map_err(Error::Select)),
            _t: PhantomData,
        }
    }
    pub fn client(&mut self) -> &mut Client {
        &mut self.client
    }
}

pub trait PsqlDeserializeAll<'d> {
    type Item: Deserialize<'d>;
    fn query(&self) -> &str;
}

impl<'s,'d,Q> PsqlDeserialize<'s,'d> for Q where Q: PsqlDeserializeAll<'d> {
    type Item = <Q as PsqlDeserializeAll<'d>>::Item;
    type Iter = std::iter::Empty<&'s dyn ToSql>;
    type IntoIter = Self::Iter;

    fn query(&self) -> &str {
        PsqlDeserializeAll::query(self)
    }
    fn params(&self) -> Self::IntoIter {
        std::iter::empty()
    }
}

pub trait PsqlDeserialize<'s,'d>   {
    type Item: Deserialize<'d>;
    type Iter: Iterator<Item = &'s dyn ToSql> + ExactSizeIterator;
    type IntoIter: IntoIterator<Item = &'s dyn ToSql, IntoIter = Self::Iter>;

    fn query(&'s self) -> &'s str;
    fn params(&'s self) -> Self::IntoIter;
}

pub struct CastOptions {
    allow_itou: bool,
}
impl CastOptions {
    pub fn new() -> CastOptions {
        CastOptions {
            allow_itou: false,
        }
    }
    pub fn allow_itou(&mut self) -> &mut Self {
        self.allow_itou = true;
        self
    }
    pub fn cast<'t,T>(&self, rows: RowIter<'t>) -> ResultIterator<'t,T> {
        ResultIterator {
            allow_itou: self.allow_itou,
            iter: Some(Ok(rows)),
            _t: PhantomData,
        }
    }
}

pub struct ResultIterator<'t,T> {
    allow_itou: bool,
    iter: Option<Result<RowIter<'t>,Error>>,
    _t: PhantomData<T>,
}
impl<'t,T> Iterator for ResultIterator<'t,T>
where T: Deserialize<'t>
{
    type Item = Result<T,Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.take() {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(mut rows)) => match rows.next().map_err(Error::SelectNext) {
                Err(e) => Some(Err(e)),
                Ok(None) => None,
                Ok(Some(row)) => {
                    let mut deserializer = Deserializer::from_row(&row);
                    if self.allow_itou {
                        deserializer = deserializer.allow_itou();
                    }
                    let r = T::deserialize(&mut deserializer);
                    if r.is_ok() {
                        self.iter = Some(Ok(rows));
                    }
                    Some(r)
                }
            }, 
        }
    }
}

