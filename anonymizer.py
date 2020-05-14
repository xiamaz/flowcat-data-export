import json
import struct
from pathlib import Path
import hashlib
import re

from flowcat import utils, io_functions


NAME = "anonymizer"
LOGGER = utils.setup_logging(utils.URLPath(f"logs/{NAME}_{utils.create_stamp()}.log"), NAME)


class FCSException(Exception):
    pass


def passthrough(bytedata, dest):
    dest.extend(bytedata)


def parse_num_field(data, offset, width):
    bindata, = struct.unpack_from(f"{width}s", data, offset)
    strdata = bindata.decode().strip()
    if strdata:
        result = int(strdata)
    else:
        result = 0
    return result


class FCSHeader:
    def __init__(self, bytedata):
        if len(bytedata) < 58:
            raise FCSException("Too little data")

        text_start = parse_num_field(bytedata, 10, 8)
        text_end = parse_num_field(bytedata, 18, 8)
        self.text = (text_start, text_end)
        data_start = parse_num_field(bytedata, 26, 8)
        data_end = parse_num_field(bytedata, 34, 8)
        self.data = (data_start, data_end)
        analysis_start = parse_num_field(bytedata, 42, 8)
        analysis_end = parse_num_field(bytedata, 50, 8)
        self.analysis = (analysis_start, analysis_end)

    def __repr__(self):
        return f"<Header {self.text} {self.data} {self.analysis}>"


BLANKED_KEYS = [
    b"@SAMPLEID1",
    b"@SAMPLEID2",
    b"@SAMPLEID3",
    b"@SAMPLEID4",
    b"$INST",
    b"$INSTADDRESS",
    b"@LOCATION",
    b"$FIL",
]


class FCSText:
    def __init__(self, bytedata):
        sep = bytedata[0:1]
        cleaned = [b""]
        parts = bytedata.split(sep)

        self.acquisition_offset = None

        for key, value in zip(parts[1::2], parts[2::2]):
            if key == b"$NEXTDATA":
                self.next_data = int(value)
            if key == b"@Acquisition Protocol Offset":
                self.acquisition_offset = int(value)
            elif key in BLANKED_KEYS:
                value = b" " * len(value)
            cleaned.append(key)
            cleaned.append(value)
        self.data = sep.join(cleaned)

    def __len__(self):
        return len(self.data)


class FCS:
    def __init__(self, bytedata):
        self.header = FCSHeader(bytedata[0:58])
        self.text = FCSText(
            bytedata[self.header.text[0]:self.header.text[1]])
        if self.text.next_data:
            # print(self.text.next_data - self.header.data[1])
            # print(self.text.next_data - self.text.acquisition_offset)
            # print(bytedata[self.header.data[1]:self.text.next_data])
            # print(bytedata[self.text.acquisition_offset:self.text.next_data])
            self.data = bytearray(bytedata[:self.text.next_data])
            self.next = self.__class__(bytedata[self.text.next_data:])
        else:
            self.data = bytearray(bytedata)
            self.next = None

        # Null acquisition protocol
        if self.text.acquisition_offset:
            ac_offset = self.text.acquisition_offset
            if ac_offset < self.header.data[1] or ac_offset < self.header.text[1]:
                raise RuntimeError("Acquisition protocol not after data. This is not handled yet.")
            ac_length = self.text.next_data - ac_offset

            self.data[ac_offset:self.text.next_data] = b"\x00" * ac_length

        self.data[self.header.text[0]:self.header.text[1]] = self.text.data


def write_fcs(fcsdata, destpath):
    """Write fcs file including all multipart data.
    Does not fully comply with FCS 3.0"""
    written = 0
    with open(destpath, "wb") as destfile:
        while fcsdata is not None:
            written += destfile.write(fcsdata.data)
            fcsdata = fcsdata.next
    return written


def read_fcs(srcpath):
    """Read fcs file from given origin."""
    with open(srcpath, "rb") as sfile:
        data = sfile.read() 
    fcsdata = FCS(data)
    return fcsdata


def print_fcs(fcsdata):
    while fcsdata is not None:
        print(fcsdata.header)
        fcsdata = fcsdata.next


def anon_move(orig, dest):
    """Save to new location with removed SAMPLEID and FIL."""
    data = read_fcs(orig)
    write_fcs(data, dest)


def main():
    # dataset = io_functions.load_case_collection(
    #     utils.URLPath("/data/flowcat-data/mll-flowdata/decCLL-9F"),
    #     utils.URLPath("/data/flowcat-data/mll-flowdata/decCLL-9F.2019-10-29.meta/test.json.gz")
    # )
    dataset = io_functions.load_case_collection(
        utils.URLPath("/data/flowcat-data/paper-cytometry/unused-data"),
    )

    LOGGER.info("Anonymizing dataset: %s", dataset)

    OUTPUT = utils.URLPath("/data/flowcat-data/paper-cytometry-resubmit/unused_data_anonymized")


    data_dir = OUTPUT / "data"
    data_dir.mkdir()

    for case in dataset:
        # if case.id != "ffc59330acb49e6fcf5e679dbabcd01e56991345":
        #     continue

        for sample in case.samples:
            old_path = sample.complete_path
            new_path = data_dir / sample.path

            LOGGER.info("Saving %s sample to %s", case.id, new_path)

            new_path.parent.mkdir()
            anon_move(str(old_path), str(new_path))

    # io_functions.save_case_collection(dataset, OUTPUT / "meta.json.gz")


if __name__ == "__main__":
    main()
