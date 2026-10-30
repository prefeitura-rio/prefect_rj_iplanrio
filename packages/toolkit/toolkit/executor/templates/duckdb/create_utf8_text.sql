{#
{
  "kind": "template",
  "description": "Define utf8_text, which turns the Windows-1252 text of a SQL Server column into UTF-8. A text that is already valid UTF-8 and any other type pass through. Overlong forms and surrogates are not valid UTF-8.",
  "inputs": {}
}
#}
-- noqa: disable=PRS
CREATE OR REPLACE MACRO utf8_text(text VARCHAR) AS
    CASE
        WHEN text IS NULL THEN NULL
        WHEN regexp_full_match(
            hex(encode(text)),
            '(?:[0-7][0-9A-F]|(?:C[2-9A-F]|D[0-9A-F])[89AB][0-9A-F]|E0[AB][0-9A-F][89AB][0-9A-F]|(?:E[1-9A-C]|E[EF])(?:[89AB][0-9A-F]){2}|ED[89][0-9A-F][89AB][0-9A-F]|F0[9AB][0-9A-F](?:[89AB][0-9A-F]){2}|F[1-3](?:[89AB][0-9A-F]){3}|F48[0-9A-F](?:[89AB][0-9A-F]){2})*'
        ) THEN text
        ELSE array_to_string(
            list_transform(
                regexp_extract_all(hex(encode(text)), '..'),
                byte -> CASE
                    WHEN ('0x' || byte)::INTEGER BETWEEN 128 AND 159
                        THEN list_extract(
                            ['€', chr(129), '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', chr(141), 'Ž', chr(143),
                             chr(144), '‘', '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', chr(157), 'ž', 'Ÿ'],
                            ('0x' || byte)::INTEGER - 127
                        )
                    ELSE chr(('0x' || byte)::INTEGER)
                END
            ),
            ''
        )
    END,
    (value) AS value;
