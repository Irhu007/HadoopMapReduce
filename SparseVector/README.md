#Inner product of two sparse vectors—
A vector is a list of values. Given two vectors, 
X = [x1, x2, ...] and Y = [y1, y2, ...], their inner product is Z = x1 * y1 + 
x2 * y2 + ... . When X and Y are long but have many elements with zero value, 
they’re usually given in a sparse representation: 
1,0.46
9,0.21
17,0.92
...
where the key (fi
 rst column) is the index into the vector. All elements not 
explicitly specifi
 ed are considered to have a value of zero.
