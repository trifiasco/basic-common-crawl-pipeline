from commoncrawl_pipeline.commoncrawl import CSVIndexReader


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756\tcdx-00000.gz\t0\t188224\t1\n\
101,141,199,66)/robots.txt 20240714155331\tcdx-00000.gz\t188224\t178351\t2\n\
104,223,1,100)/ 20240714230020\tcdx-00000.gz\t366575\t178055\t3"
    filename.write_text(index)
    with CSVIndexReader(str(filename)) as reader:
        assert list(reader) == [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
