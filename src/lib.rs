use minitrace::{Properties, Span, State, TraceDetails};
use rand::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

mod zigzag;

thread_local! {
   static TRACE_ID_HIGH: i64 = random();
   static TRACE_ID_LOW: RefCell<i64> = RefCell::new(0);
}

// return ([bytes], id -> &[bytes])
fn reorder_properties(p: &Properties) -> (Vec<(u64, &[u8])>, HashMap<u64, (usize, usize)>) {
    if p.span_ids.is_empty() || p.property_lens.is_empty() {
        return (vec![], HashMap::new());
    }
    assert_eq!(p.span_ids.len(), p.property_lens.len());

    let mut id_bytes_pairs = Vec::with_capacity(p.span_ids.len());
    {
        let mut remainder_bytes = p.payload.as_slice();
        for (id, len) in p.span_ids.iter().zip(p.property_lens.iter()) {
            let (bytes, remainder) = remainder_bytes.split_at(*len as _);
            remainder_bytes = remainder;
            id_bytes_pairs.push((*id, bytes));
        }

        id_bytes_pairs.sort_unstable_by_key(|s| s.0);
    }

    let mut id_to_bytes_slice = HashMap::with_capacity(id_bytes_pairs.len());
    {
        let mut current_id = id_bytes_pairs[0].0;
        let mut current_index = 0;
        let mut len = 0;

        for (index, &(id, _)) in id_bytes_pairs.iter().enumerate() {
            if id == current_id {
                len += 1;
            } else {
                id_to_bytes_slice.insert(current_id, (current_index, len));

                current_id = id;
                current_index = index;
                len = 1;
            }
        }
        id_to_bytes_slice.insert(current_id, (current_index, len));
    }

    (id_bytes_pairs, id_to_bytes_slice)
}

pub fn thrift_encode(
    buf: &mut Vec<u8>,
    service_name: &str,
    TraceDetails {
        start_time_ns,
        cycles_per_second,
        spans,
        properties,
        ..
    }: &TraceDetails,
    event_to_operation_name: impl Fn(&u32) -> String,
) {
    let trace_id_high = TRACE_ID_HIGH.with(|h| *h);
    let trace_id_low = TRACE_ID_LOW.with(|l| {
        *l.borrow_mut() += 1;
        *l.borrow()
    });
    let (bytes_slices, id_to_bytes_slice) = reorder_properties(properties);
    let start_time_us = *start_time_ns / 1_000;

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
    // const PROCESS_DELTA: u8 = 1;
    // const STRUCT_TYPE: u8 = 12;
    // const PROCESS_TYPE: u8 = PROCESS_DELTA << 4 | STRUCT_TYPE;
    // buf.push(PROCESS_TYPE);
    // ```
    //
    // ## service name field header
    // ```
    // const SERVICE_NAME_DELTA: u8 = 1;
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
    // const SPANS_DELTA: u8 = 1;
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

    let anchor_cycles = spans
        .iter()
        .find(|s| s.state == State::Root)
        .expect("not contain root span")
        .begin_cycles;

    for span in spans {
        let Span {
            id,
            state,
            related_id,
            begin_cycles,
            elapsed_cycles,
            event,
        } = span;

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
        // buf.push(TRACE_ID_HIGH_TYPE);operation_name
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
        write_varint(buf, zigzag::from_i64(*id as _));

        // parent span id field header
        // ```
        // const PARENT_SPAN_ID_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const PARENT_SPAN_ID_TYPE: u8 = (PARENT_SPAN_ID_DELTA << 4) as u8 | I64_TYPE;
        // buf.push(PARENT_SPAN_ID_TYPE);
        // ```property_lens
        buf.push(0x16);
        // parent span id data
        write_varint(buf, zigzag::from_i64(*related_id as _));

        // operation name field header
        // ```
        // const OPERATION_NAME_DELTA: i16 = 1;
        // const BINARY_TYPE: u8 = 8;
        // const OPERATION_NAME_TYPE: u8 = (OPERATION_NAME_DELTA << 4) as u8 | BINARY_TYPE;
        // buf.push(OPERATION_NAME_TYPE);
        // ```
        buf.push(0x18);
        // operation name data
        format!(
            "{}{}",
            event_to_operation_name(event),
            match *state {
                State::Root => " (Root spawning)",
                State::Spawning => " (Spawning)",
                State::Scheduling => " (Scheduling)",
                _ => "",
            }
        )
        .as_bytes()
        .encode(buf);

        if *state != State::Root {
            // references field header
            // ```
            // const REFERENCES_DELTA: i16 = 1;flags
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
            write_varint(
                buf,
                zigzag::from_i32(match state {
                    State::Local => 0,      // Child of
                    State::Spawning => 1,   // Follows from
                    State::Scheduling => 1, // Follows from
                    State::Settle => 1,     // Follows from
                    State::Root => unreachable!(),
                }) as _,
            );
            // reference trace id low header
            // ```
            // const REF_TRACE_ID_LOW_DELTA: i16 = 1;
            // const I64_TYPE: u8 = 6;
            // const REF_TRACE_ID_LOW_TYPE: u8 = (REF_TRACE_ID_LOW_DELTA << 4) as u8 | I64_TYPE;
            // ```
            buf.push(0x16);
            // reference trace id low data
            write_varint(buf, zigzag::from_i64(trace_id_low));
            // reference trace id high header
            // ```
            // const REF_TRACE_ID_HIGH_DELTA: i16 = 1;
            // const I64_TYPE: u8 = 6;
            // const REF_TRACE_ID_HIGH_TYPE: u8 = (REF_TRACE_ID_HIGH_DELTA << 4) as u8 | I64_TYPE;
            // ```
            buf.push(0x16);
            // reference trace id high data
            write_varint(buf, zigzag::from_i64(trace_id_high));
            // reference span id header
            // ```
            // const SPAN_ID_HIGH_DELTA: i16 = 1;
            // const I64_TYPE: u8 = 6;
            // const SPAN_ID_HIGH_TYPE: u8 = (SPAN_ID_HIGH_DELTA << 4) as u8 property_lens
            // ```
            buf.push(0x16);
            // reference span id data
            write_varint(buf, zigzag::from_i64(*related_id as _));
            // reference struce tail
            buf.push(0x00);
        }

        // flags header
        //
        // If it's Root, the references field is not set.
        // Then, DELTA is 2.
        //
        // ```
        // const FLAGS_DELTA: i16 = if *state != State::Root {1} else {2};
        // const I32_TYPE: u8 = 5;
        // const FLAGS_TYPE: u8 = (FLAGS_DELTA << 4) as u8 | I32_TYPE;
        // ```
        if *state != State::Root {
            buf.push(0x15);
        } else {
            buf.push(0x25);
        }

        // flags data, `1` signifies a SAMPLED span, `2` signifies a DEBUG span.
        write_varint(buf, zigzag::from_i32(1) as _);

        // start time header
        // ```
        // const START_TIME_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;property_lens
        buf.push(0x16);
        // start time data
        let delta_cycles = begin_cycles - anchor_cycles;
        let delta_us = delta_cycles as f64 / *cycles_per_second as f64 * 1_000_000.0;
        write_varint(
            buf,
            zigzag::from_i64((start_time_us + delta_us as u64) as _),
        );

        // duration header
        // ```
        // const DURATION_DELTA: i16 = 1;
        // const I64_TYPE: u8 = 6;
        // const DURATION_TYPE: u8 = (DURATION_DELTA << 4) as u8 | I64_TYPE;
        // ```
        buf.push(0x16);
        // duration data
        let duration_us = *elapsed_cycles as f64 / *cycles_per_second as f64 * 1_000_000.0;
        write_varint(buf, zigzag::from_i64(duration_us as _));

        // tags
        if let Some((from, limit)) = id_to_bytes_slice.get(id) {
            // tags field header
            // ```
            // const TAGS_DELTA: i16 = 1;property_lens
            // ```
            buf.push(0x19);
            // tags list header
            let len = *limit;
            const STRUCT_TYPE: u8 = 12;
            if len < 15 {
                buf.push((len << 4) as u8 | STRUCT_TYPE as u8);
            } else {
                buf.push(0b1111_0000 | STRUCT_TYPE as u8);
                write_varint(buf, len as _);
            }

            let bytes = &bytes_slices[*from..*from + *limit];

            for (_, bytes) in bytes {
                let mut split = bytes.splitn(2, |b| *b == b':');
                let key = split.next().unwrap_or(&[]);
                let value = split.next().unwrap_or(&[]);

                // key field header
                // ```
                // const KEY_DELTA: i16 = 1;
                // const BINARY_TYPE: u8 = 8;
                // const KEY_TYPE: u8 = (KEY_DELTA << 4) as u8 | BYTES_TYPE;
                // ```
                buf.push(0x18);
                // key dataproperty_lens
                key.encode(buf);

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
                value.encode(buf);

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
    fn it_work() {
        let res = {
            let (_g, collector) = minitrace::trace_enable(0u32);
            minitrace::property(b"test property:a root span");

            std::thread::sleep(std::time::Duration::from_millis(20));

            {
                let _g = minitrace::new_span(1u32);
                minitrace::property(b"where am i:in child");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            minitrace::property(b"another test property:done");
            collector
        }
        .collect();

        let mut buf = Vec::with_capacity(1024);
        thrift_encode(&mut buf, "test_minitrace".into(), &res, |s| {
            if *s == 0 {
                "Parent".into()
            } else {
                "Child".into()
            }
        });

        let agent = std::net::SocketAddr::from(([127, 0, 0, 1], 6831));

        let socket = std::net::UdpSocket::bind(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
            0,
        ))
        .unwrap();

        socket.send_to(&buf, agent).unwrap();
    }
}
