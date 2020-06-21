Kindle Clippings: Extract, Transform, Load
==========================================

Building on the `Kindle Clippings library`_...

.. _`Kindle Clippings library`:  https://github.com/MilkShakeYoung/kindle-clippings

- This script will **Extract** your 'My Clippings.txt' file from your Kindle
- **Transform** the plain text into Book, Quote pairs 
- **Load** the resulting text into files of quotes for each individual book


Usage
-----

1. Plug your Kindle into your computer
2. Download/Clone this directory
3. Install dependencies: `pip install -r requirements.txt`
4. Run: `python kindle.py`


Demo
----

Example output files tree:

.. code:: bash

    $ tree .
    .
    ├── My Clippings.txt
    ├── README.rst
    ├── requirements.txt
    ├── kindle.py
    └── output
        ├── Hackers & Painters (Paul Graham).txt
        ├── Life of Pi (Yann Martel).txt

Example output file contents:

    Nerds aren't losers. They're just playing a different game, and a game much closer to the one played in the real world. Adults know this. It's hard to find successful adults now who don't claim to have been nerds in high school.

    ---

    What hackers and painters have in common is that they're both makers. Along with composers, architects, and writers, what hackers and painters are trying to do is make good things. They're not doing research per se, though if in the course of trying to make good things they discover some new technique, so much the better.

    ---

    This is not a problem for big companies, because they don't win by making great products. Big companies win by sucking less than other big companies.

