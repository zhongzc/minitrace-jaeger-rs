mod zigzag;

enum SpanRefKind {
    ChildOf = 0,
    FollowsFrom = 1,
}

struct SpanRef {
    kind: SpanRefKind,
    trace_id_low: i64,
    trace_id_high: i64,
    span_id: i64,
}

struct Tag {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct Span {
    trace_id_low: i64,      // 1
    trace_id_high: i64,     // 2
    span_id: i64,           // 3
    parent_span_id: i64,    // 4
    operation_name: String, // 5
    reference: SpanRef,     // 6
    flags: i32,             // 7  `1` signifies a SAMPLED span, `2` signifies a DEBUG span.
    start_time: i64,        // 8
    duration: i64,          // 9
    tags: Vec<Tag>,         // 10
}

fn encode(buf: &mut Vec<u8>, service_name: &str, spans: Vec<Span>) {
    // # thrift message header
    // ## protocal id
    // ```
    // const COMPACT_PROTOCOL_ID: u8 = 0x82;
    // buf.push(COMPACT_PROTOCOL_ID);
    // ```
    //
    // ## compact & oneway
    // ```
    // const ONEWAY: u8 = 4;
    // const COMPACT_PROTOCOL_VERSION: u8 = 1;
    // buf.push(ONEWAY << 5 | COMPACT_PROTOCOL_VERSION);
    // ```
    //
    // ## sequence id
    // ```
    // const SEQUENCE_ID: u8 = 0;
    // buf.push(SEQUENCE_ID);
    // ```
    //
    // ## method name
    // ```
    // const METHOD_NAME: &str = "emitBatch";
    // METHOD_NAME.as_bytes().encode(buf);
    // ```
    //
    // # batch struct
    // ## batch header
    // ```
    // const DELTA: u8 = 1;
    // const STRUCT_TYPE: u8 = 12;
    // const FIELD_STRUCT: u8 = DELTA << 4 | STRUCT_TYPE;
    // buf.push(FIELD_STRUCT);
    // ```
    //
    // ## process field header
    // ```
    // const PROCESS_FIELD_ID: i16 = 1;
    // const PROCESS_DELTA: u8 = PROCESS_FIELD_ID as u8;
    // const STRUCT_TYPE: u8 = 12;
    // const PROCESS_TYPE: u8 = PROCESS_DELTA << 4 | STRUCT_TYPE;
    // buf.push(PROCESS_TYPE);
    // ```
    //
    // ## service name field header
    // ```
    // const SERVICE_NAME_FIELD_ID: i16 = 1;
    // const SERVICE_NAME_DELTA: u8 = SERVICE_NAME_FIELD_ID as u8;
    // const BINARY_TYPE: u8 = 8;
    // const SERVICE_NAME_TYPE: u8 = SERVICE_NAME_DELTA << 4 | BINARY_TYPE;
    // buf.push(SERVICE_NAME_TYPE);
    buf.extend_from_slice(&[
        0x82, 0x81, 0x00, 0x09, 0x65, 0x6d, 0x69, 0x74, 0x42, 0x61, 0x74, 0x63, 0x68, 0x1c, 0x1c,
        0x18,
    ]);

    // service name string
    service_name.as_bytes().encode(buf);

    // process tail
    //
    // NOTE: ignore tags
    buf.push(0x00);

    // spans field header
    //
    // ```
    // const SPANS_FIELD_ID: i16 = 2;
    // const SPANS_DELTA: u8 = (SPANS_FIELD_ID - 1) as u8;
    // const LIST_TYPE: u8 = 9;
    // const SPANS_TYPE: u8 = SPANS_DELTA << 4 | LIST_TYPE;
    // buf.push(SPANS_TYPE);
    // ```
    buf.push(0x19);

    // spans list header
    let len = spans.len();
    const STRUCT_TYPE: u8 = 12;
    if len < 15 {
        buf.push((len << 4) as u8 | STRUCT_TYPE as u8);
    } else {
        buf.push(0b1111_0000 | STRUCT_TYPE as u8);
        write_varint(buf, len as _);
    }

    for Span {
        trace_id_low,
        trace_id_high,
        span_id,
        parent_span_id,
        operation_name,
        reference:
            SpanRef {
                kind: ref_kind,
                trace_id_low: ref_trace_id_low,
                trace_id_high: ref_trace_id_high,
                span_id: ref_span_id,
            },
        flags,
        start_time,
        duration,
        tags,
    } in spans
    {
        // trace id low field header
        // ```
        // const TRACE_ID_LOW_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const TRACE_ID_LOW_TYPE: u8 = (TRACE_ID_LOW_DELTA << 4) as u8 | I64_TYPE;
        // buf.push(TRACE_ID_LOW_TYPE);
        // ```
        buf.push(0x16);
        // trace id low data
        write_varint(buf, zigzag::from_i64(trace_id_low));

        // trace id high field header
        // ```ref_kind
        // const TRACE_ID_HIGH_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const TRACE_ID_HIGH_TYPE: u8 = (TRACE_ID_HIGH_DELTA << 4) as u8 | I64_TYPE;
        // buf.push(TRACE_ID_HIGH_TYPE);
        // ```
        buf.push(0x16);
        // trace id high data
        write_varint(buf, zigzag::from_i64(trace_id_high));

        // span id field header
        // ```
        // const SPAN_ID_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const SPAN_ID_TYPE: u8 = (SPAN_ID_DELTA << 4) as u8 | I64_TYPE;
        // buf.push(SPAN_ID_TYPE);
        // ```
        buf.push(0x16);
        // span id data
        write_varint(buf, zigzag::from_i64(span_id));

        // parent span id field header
        // ```
        // const PARENT_SPAN_ID_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const PARENT_SPAN_ID_TYPE: u8 = (PARENT_SPAN_ID_DELTA << 4) as u8 | I64_TYPE;
        // buf.push(PARENT_SPAN_ID_TYPE);
        // ```
        buf.push(0x16);
        // parent span id data
        write_varint(buf, zigzag::from_i64(parent_span_id));

        // operation name field header
        // ```
        // const OPERATION_NAME_DELTA: i16 = 1;
        // const BINARY_TYPE: u8 = 8;
        // const OPERATION_NAME_TYPE: u8 = (OPERATION_NAME_DELTA << 4) as u8 | BINARY_TYPE;
        // buf.push(OPERATION_NAME_TYPE);
        // ```
        buf.push(0x18);
        // operation name data
        operation_name.as_bytes().encode(buf);

        // references field header
        // ```
        // const REFERENCES_DELTA: i16 = 1;
        // const LIST_TYPE: u8 = 9;
        // const REFERENCES_TYPE: u8 = (REFERENCES_DELTA << 4) as u8 | LIST_TYPE;
        // buf.push(REFERENCES_TYPE);
        // ```
        buf.push(0x19);
        // references list header
        // NOTE: only one reference
        // ```
        // const STRUCT_TYPE: u8 = 12;
        // let HEADER = (1 << 4) as u8 | STRUCT_TYPE as u8;
        // buf.push(HEADER);
        // ```
        buf.push(0x1c);
        // reference kind header
        // ```
        // const REF_KIND_DELTA: i16 = 1;
        // const I32_TYPE: u8 = 5;
        // const REF_KIND_TYPE: u8 = (REF_KIND_DELTA << 4) as u8 | I32_TYPE;
        // ```
        buf.push(0x15);
        // reference kind data
        write_varint(buf, zigzag::from_i32(ref_kind as _) as _);
        // reference trace id low header
        // ```
        // const REF_TRACE_ID_LOW_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const REF_TRACE_ID_LOW_TYPE: u8 = (REF_TRACE_ID_LOW_DELTA << 4) as u8 | I64_TYPE;
        // ```
        buf.push(0x16);
        // reference trace id low data
        write_varint(buf, zigzag::from_i64(ref_trace_id_low));
        // reference trace id high header
        // ```
        // const REF_TRACE_ID_HIGH_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const REF_TRACE_ID_HIGH_TYPE: u8 = (REF_TRACE_ID_HIGH_DELTA << 4) as u8 | I64_TYPE;
        // ```
        buf.push(0x16);
        // reference trace id high data
        write_varint(buf, zigzag::from_i64(ref_trace_id_high));
        // reference span id header
        // ```
        // const SPAN_ID_HIGH_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const SPAN_ID_HIGH_TYPE: u8 = (SPAN_ID_HIGH_DELTA << 4) as u8 | I64_TYPE;
        // ```
        buf.push(0x16);
        // reference span id data
        write_varint(buf, zigzag::from_i64(ref_span_id));
        // reference struce tail
        buf.push(0x00);

        // flags header
        // ```
        // const FLAGS_DELTA: i16 = 1;
        // const I32_TYPE: u8 = 5;
        // const FLAGS_TYPE: u8 = (FLAGS_DELTA << 4) as u8 | I32_TYPE;
        // ```
        buf.push(0x15);
        // flags data
        write_varint(buf, zigzag::from_i32(flags) as _);

        // start time header
        // ```
        // const START_TIME_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const START_TIME_TYPE: u8 = (START_TIME_DELTA << 4) as u8 | I64_TYPE;
        // ```
        buf.push(0x16);
        // start time data
        write_varint(buf, zigzag::from_i64(start_time));

        // duration header
        // ```
        // const DURATION_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const DURATION_TYPE: u8 = (DURATION_DELTA << 4) as u8 | I64_TYPE;
        // ```
        buf.push(0x16);
        // duration data
        write_varint(buf, zigzag::from_i64(duration));

        // tags
        if !tags.is_empty() {
            // tags field header
            // ```
            // const TAGS_DELTA: i16 = 1;
            // const LIST_TYPE: u8 = 9;
            // const TAGS_TYPE: u8 = (TAGS_DELTA << 4) as u8 | LIST_TYPE;
            // buf.push(TAGS_TYPE);
            // ```
            buf.push(0x19);
            // tags list header
            let len = tags.len();
            const STRUCT_TYPE: u8 = 12;
            if len < 15 {
                buf.push((len << 4) as u8 | STRUCT_TYPE as u8);
            } else {
                buf.push(0b1111_0000 | STRUCT_TYPE as u8);
                write_varint(buf, len as _);
            }

            for Tag { key, value } in tags {
                // key field header
                // ```
                // const KEY_DELTA: i16 = 1;
                // const BINARY_TYPE: u8 = 8;
                // const KEY_TYPE: u8 = (KEY_DELTA << 4) as u8 | BYTES_TYPE;
                // ```
                buf.push(0x18);
                // key data
                key.as_slice().encode(buf);

                // type field header
                // ```
                // const TYPE_DELTA: i16 = 1;
                // const I32_TYPE: u8 = 5;
                // const TYPE_TYPE: u8 = (TYPE_DELTA << 4) as u8 | BYTES_TYPE;
                // ```
                buf.push(0x15);
                // type data;
                buf.push(0);

                // value field header
                // ```
                // const VALUE_DELTA: i16 = 1;
                // const BINARY_TYPE: u8 = 8;
                // const VALUE_TYPE: u8 = (VALUE_DELTA << 4) as u8 | BYTES_TYPE;
                // ```
                buf.push(0x18);
                // value data
                value.as_slice().encode(buf);

                // tag struct tail
                buf.push(0x00);
            }
        }

        // span struct tail
        buf.push(0x00);
    }

    // spans struct tail
    buf.push(0x00);
    // batch struct tail
    buf.push(0x00);
}

trait Encode {
    fn encode(&self, buf: &mut Vec<u8>);
}

impl<'a> Encode for &'a [u8] {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_varint(buf, self.len() as _);
        buf.extend_from_slice(self);
    }
}

fn write_varint(buf: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut b = (n & 0b0111_1111) as u8;
        n >>= 7;
        if n != 0 {
            b |= 0b1000_0000;
        }
        buf.push(b);
        if n == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        let mut buf = vec![];
        encode(
            &mut buf,
            "minitrace_demo",
            vec![
                Span {
                    trace_id_low: 10,
                    trace_id_high: 20,
                    span_id: 30,
                    parent_span_id: 2000,
                    operation_name: "22".into(),
                    reference: SpanRef {
                        kind: SpanRefKind::ChildOf,
                        trace_id_low: 80,
                        trace_id_high: 40,
                        span_id: 0,
                    },
                    flags: 1,
                    start_time: 606060606,
                    duration: 30303030,
                    tags: vec![
                        Tag {
                            key: "abc".into(),
                            value: "efg".into(),
                        },
                        Tag {
                            key: "13579".into(),
                            value: "24680".into(),
                        },
                    ],
                },
                Span {
                    trace_id_low: 498465402135,
                    trace_id_high: 89765413645,
                    span_id: 514213548979,
                    parent_span_id: 5454564654,
                    operation_name: "226545sdfslf".into(),
                    reference: SpanRef {
                        kind: SpanRefKind::FollowsFrom,
                        trace_id_low: 35465432,
                        trace_id_high: 5468748964,
                        span_id: 564654,
                    },
                    flags: 2,
                    start_time: 82374287346238,
                    duration: 34897238974234,
                    tags: vec![],
                },
            ],
        );
        println!("{}", hex::encode(buf));
    }
}
