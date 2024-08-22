// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::LazyLock;
use std::vec::Vec;

const LUT_2: [u16; 16] = [
    1, 256, 770, 512, 0, 771, 257, 513, 514, 258, 768, 3, 515, 769, 259, 2,
];

const LUT_3: [u16; 96] = [
    1, 258, 771, 514, 1797, 1540, 1027, 1284, 2, 1798, 256, 1543, 776, 1032, 512, 1287, 0, 777,
    1802, 1033, 257, 513, 1547, 1291, 518, 768, 262, 11, 1289, 1024, 1545, 1800, 1541, 1285, 264,
    520, 1792, 1030, 10, 774, 1035, 779, 1280, 519, 1796, 9, 1536, 263, 1028, 1799, 772, 3, 1281,
    1537, 523, 267, 1542, 1793, 1286, 1034, 265, 5, 521, 778, 522, 1283, 769, 1025, 266, 1539, 4,
    1801, 517, 261, 1288, 1544, 770, 7, 1026, 1795, 1031, 1282, 1803, 1538, 775, 516, 8, 260, 1546,
    259, 1794, 6, 1290, 515, 1029, 773,
];

const LUT_4: [u16; 512] = [
    1, 258, 771, 514, 1797, 1540, 1027, 1284, 3849, 3592, 3079, 3336, 2053, 2310, 2823, 2566, 2,
    3850, 267, 3596, 781, 3086, 523, 3340, 1807, 2063, 1552, 2321, 1037, 2830, 1296, 2577, 11,
    1810, 3859, 2066, 256, 1556, 3605, 2326, 791, 1047, 3096, 2840, 512, 1300, 3349, 2582, 1032,
    1809, 776, 22, 2822, 2065, 3078, 3854, 1281, 1537, 537, 281, 2586, 2330, 3355, 3611, 3612,
    2309, 269, 1550, 3851, 2058, 19, 1802, 3356, 2565, 525, 1294, 3081, 2825, 783, 1039, 1546,
    1792, 1290, 1043, 274, 26, 530, 787, 2333, 2048, 2589, 2846, 3615, 3863, 3359, 3102, 540, 1285,
    3341, 2574, 769, 1025, 3097, 2841, 284, 1541, 3597, 2318, 16, 1821, 3870, 2077, 2569, 2313,
    3343, 3599, 1303, 1559, 536, 280, 2818, 2060, 3074, 3862, 1028, 1804, 772, 14, 3098, 2842, 795,
    1051, 3328, 2580, 533, 1302, 3856, 2079, 30, 1823, 3584, 2324, 277, 1558, 2073, 1817, 2315,
    1548, 2829, 1038, 2571, 1292, 3848, 29, 3600, 273, 3085, 782, 3344, 529, 2052, 3852, 1796, 3,
    2304, 3604, 1557, 278, 2839, 3095, 1048, 792, 2560, 3348, 1301, 534, 0, 797, 1822, 1053, 3868,
    3103, 2078, 2847, 257, 513, 1561, 1305, 3610, 3354, 2331, 2587, 3080, 3860, 2824, 2055, 774, 5,
    1030, 1799, 3329, 3585, 2585, 2329, 538, 282, 1307, 1563, 1036, 1282, 1814, 1538, 780, 516, 27,
    260, 2833, 2568, 2070, 2312, 3089, 3334, 3864, 3590, 2570, 2827, 2314, 2069, 3346, 3083, 3602,
    3867, 1309, 1040, 1565, 1813, 543, 784, 287, 24, 1555, 2307, 1793, 2049, 1299, 2563, 1052,
    2821, 286, 3591, 8, 3869, 542, 3335, 796, 3077, 3593, 3337, 2319, 2575, 279, 535, 1560, 1304,
    3840, 3082, 2067, 2826, 28, 786, 1811, 1042, 521, 265, 1295, 1551, 3351, 3607, 2584, 2328, 770,
    20, 1026, 1795, 3076, 3845, 2820, 2051, 1564, 261, 2317, 3598, 1794, 12, 2050, 3843, 1308, 517,
    2573, 3342, 1033, 777, 2831, 3087, 2064, 2845, 3861, 3101, 1808, 1055, 13, 799, 2305, 2561,
    3609, 3353, 1562, 1306, 283, 539, 3594, 3841, 3338, 3091, 2322, 2076, 2578, 2835, 285, 9, 541,
    798, 1567, 1820, 1311, 1054, 3084, 3330, 3865, 3586, 2828, 2564, 2062, 2308, 785, 520, 15, 264,
    1041, 1286, 1806, 1542, 522, 779, 266, 25, 1298, 1035, 1554, 1805, 3357, 3088, 3613, 3855,
    2591, 2832, 2335, 2061, 1045, 2838, 1291, 2572, 1819, 2075, 1547, 2316, 789, 3094, 528, 3345,
    6, 3871, 272, 3601, 2579, 1283, 2816, 1044, 2323, 1539, 2074, 1818, 3358, 519, 3072, 788, 3614,
    263, 3846, 31, 3603, 259, 3842, 10, 3347, 515, 3100, 773, 2334, 1543, 2057, 1801, 2590, 1287,
    2844, 1029, 3093, 790, 3339, 524, 3844, 18, 3595, 268, 2837, 1046, 2576, 1297, 2072, 1816,
    2320, 1553, 531, 3331, 768, 3092, 275, 3587, 4, 3858, 1310, 2567, 1024, 2836, 1566, 2311, 1815,
    2071, 2068, 2306, 2819, 2562, 3866, 3588, 3075, 3332, 1812, 1544, 1031, 1288, 23, 262, 775,
    518, 1050, 794, 2843, 3099, 1280, 532, 2581, 3350, 1798, 17, 2054, 3847, 1536, 276, 2325, 3606,
    1545, 1289, 271, 527, 2327, 2583, 3608, 3352, 1803, 1034, 21, 778, 2059, 2834, 3853, 3090,
    2588, 3333, 1293, 526, 2817, 3073, 1049, 793, 2332, 3589, 1549, 270, 2056, 3857, 1800, 7,
];

const LUT_5: [u16; 2560] = [
    1, 258, 771, 514, 1797, 1540, 1027, 1284, 3849, 3592, 3079, 3336, 2053, 2310, 2823, 2566, 7953,
    7696, 7183, 7440, 6157, 6414, 6927, 6670, 4105, 4362, 4875, 4618, 5901, 5644, 5131, 5388, 2,
    7954, 275, 7700, 789, 7190, 531, 7444, 1815, 6168, 1561, 6426, 1045, 6934, 1305, 6682, 3867,
    4123, 3612, 4381, 3102, 4895, 3356, 4637, 2071, 5912, 2336, 5665, 2846, 5151, 2592, 5409, 19,
    3874, 7971, 4130, 292, 3621, 7718, 4391, 808, 3113, 7210, 4907, 548, 3365, 7462, 4647, 1836,
    2092, 6189, 5933, 1582, 2351, 6448, 5681, 1064, 2857, 6954, 5163, 1326, 2607, 6704, 5425, 2056,
    3866, 1800, 39, 5894, 4122, 6150, 7958, 2304, 3634, 1587, 308, 5685, 4406, 6455, 7736, 2873,
    3129, 1082, 826, 5179, 4923, 6972, 7228, 2560, 3378, 1331, 564, 5429, 4662, 6711, 7480, 7741,
    4357, 277, 3606, 7955, 4114, 35, 3858, 7485, 4613, 533, 3350, 7230, 4873, 791, 3096, 6463,
    5645, 1566, 2335, 6161, 5905, 1819, 2075, 6719, 5389, 1310, 2591, 6974, 5129, 1047, 2840, 3088,
    3887, 2832, 2063, 782, 54, 1038, 1807, 4874, 4143, 5130, 5899, 7180, 7977, 6924, 6155, 3329,
    3585, 2624, 2368, 577, 321, 1346, 1602, 4675, 4419, 5444, 5700, 7493, 7749, 6726, 6470, 573,
    3333, 7445, 4630, 768, 3122, 7219, 4916, 317, 3589, 7701, 4374, 25, 3911, 8008, 4167, 1343,
    2573, 6686, 5407, 1024, 2866, 6963, 5172, 1599, 2317, 6430, 5663, 1849, 2105, 6202, 5946, 5694,
    4361, 6423, 7704, 2344, 3625, 1578, 299, 5890, 4116, 6146, 7975, 2052, 3860, 1796, 22, 5438,
    4617, 6679, 7448, 2600, 3369, 1322, 555, 5137, 4881, 6939, 7195, 2860, 3116, 1069, 813, 7221,
    4918, 823, 3128, 7460, 4645, 550, 3367, 7961, 4169, 72, 3913, 7716, 4389, 294, 3623, 6965,
    5174, 1079, 2872, 6702, 5423, 1328, 2609, 6203, 5947, 1852, 2108, 6446, 5679, 1584, 2353, 3602,
    3840, 3346, 3107, 2338, 2109, 2594, 2851, 327, 67, 583, 840, 1609, 1853, 1353, 1096, 4426,
    4096, 4682, 4939, 5708, 5951, 5452, 5195, 7757, 7993, 7501, 7246, 6479, 6207, 6735, 6990, 1077,
    2870, 6967, 5176, 1316, 2597, 6694, 5415, 1861, 2117, 6214, 5958, 1572, 2341, 6438, 5671, 821,
    3126, 7223, 4920, 558, 3375, 7472, 4657, 32, 3919, 8014, 4175, 302, 3631, 7728, 4401, 2622,
    3337, 1303, 536, 5416, 4649, 6698, 7467, 2817, 3073, 1088, 832, 5185, 4929, 6978, 7234, 2366,
    3593, 1559, 280, 5672, 4393, 6442, 7723, 2064, 3869, 1808, 49, 5902, 4125, 6158, 7967, 6717,
    5381, 1301, 2582, 6912, 5170, 1075, 2868, 6461, 5637, 1557, 2326, 6211, 5955, 1860, 2116, 7487,
    4621, 542, 3359, 7168, 4914, 819, 3124, 7743, 4365, 286, 3615, 7968, 4173, 78, 3917, 4625,
    4369, 5403, 5659, 7468, 7724, 6701, 6445, 3385, 3641, 2618, 2362, 571, 315, 1340, 1596, 4866,
    4133, 5122, 5891, 7172, 7990, 6916, 6147, 3080, 3877, 2824, 2055, 774, 41, 1030, 1799, 1597,
    2309, 6421, 5654, 1793, 2049, 6208, 5952, 1341, 2565, 6677, 5398, 1086, 2825, 6935, 5144, 319,
    3597, 7710, 4383, 28, 3914, 8011, 4170, 575, 3341, 7454, 4639, 830, 3081, 7191, 4888, 5187,
    4931, 6980, 7236, 2885, 3141, 1094, 838, 5376, 4658, 6707, 7476, 2613, 3382, 1335, 568, 5898,
    4129, 6154, 7985, 2060, 3873, 1804, 31, 5632, 4402, 6451, 7732, 2357, 3638, 1591, 312, 6209,
    5953, 1858, 2114, 6436, 5669, 1574, 2343, 6952, 5161, 1066, 2859, 6692, 5413, 1318, 2599, 7964,
    4172, 75, 3916, 7726, 4399, 304, 3633, 7208, 4905, 810, 3115, 7470, 4655, 560, 3377, 4160,
    3904, 4371, 3604, 4885, 3094, 4627, 3348, 5911, 2072, 5657, 2330, 5141, 2838, 5401, 2586, 7952,
    74, 7708, 285, 7198, 799, 7452, 541, 6167, 1816, 6432, 1569, 6942, 1055, 6688, 1313, 4100,
    7956, 3844, 3, 4388, 7717, 3622, 295, 4904, 7209, 3114, 811, 4644, 7461, 3366, 551, 5932, 6188,
    2093, 1837, 5678, 6447, 2352, 1585, 5160, 6953, 2858, 1067, 5422, 6703, 2608, 1329, 36, 1863,
    3912, 2119, 7997, 6217, 4168, 5961, 256, 1586, 3635, 2356, 7733, 6454, 4407, 5688, 825, 1081,
    3130, 2874, 7227, 6971, 4924, 5180, 512, 1330, 3379, 2612, 7477, 6710, 4663, 5432, 6152, 7973,
    5896, 4103, 1798, 5, 2054, 3847, 6400, 7730, 5683, 4404, 1589, 310, 2359, 3640, 6969, 7225,
    5178, 4922, 1083, 827, 2876, 3132, 6656, 7474, 5427, 4660, 1333, 566, 2615, 3384, 2076, 2890,
    3888, 3146, 1820, 1100, 55, 844, 5920, 5197, 4144, 4941, 6176, 6991, 7978, 7247, 2305, 2561,
    3648, 3392, 1601, 1345, 322, 578, 5699, 5443, 4420, 4676, 6469, 6725, 7750, 7494, 5136, 5917,
    4880, 4145, 6926, 6173, 7182, 7992, 2826, 2081, 3082, 3889, 1036, 1825, 780, 43, 5377, 5633,
    4672, 4416, 6721, 6465, 7490, 7746, 2627, 2371, 3396, 3652, 1349, 1605, 582, 326, 3092, 3330,
    3892, 3586, 2836, 2564, 2070, 2308, 794, 520, 68, 264, 1050, 1286, 1814, 1542, 4893, 4624,
    4148, 4368, 5149, 5390, 5919, 5646, 7201, 7434, 7994, 7690, 6945, 6668, 6175, 6412, 4626, 4883,
    4370, 4147, 5410, 5139, 5666, 5909, 7495, 7193, 7751, 8004, 6729, 6937, 6473, 6165, 3402, 3100,
    3658, 3891, 2636, 2844, 2380, 2078, 589, 800, 333, 58, 1359, 1056, 1615, 1822, 7742, 6409,
    4375, 5656, 296, 1577, 3626, 2347, 7972, 6162, 4131, 5906, 61, 1826, 3875, 2082, 7486, 6665,
    4631, 5400, 552, 1321, 3370, 2603, 7185, 6929, 4891, 5147, 812, 1068, 3117, 2861, 1598, 265,
    2327, 3608, 6440, 7721, 5674, 4395, 1794, 37, 2050, 3843, 6148, 7941, 5892, 4099, 1342, 521,
    2583, 3352, 6696, 7465, 5418, 4651, 1041, 785, 2843, 3099, 6956, 7212, 5165, 4909, 3619, 4355,
    3841, 4097, 3363, 4611, 3133, 4869, 2376, 5639, 2110, 5897, 2632, 5383, 2877, 5125, 331, 7695,
    16, 8010, 587, 7439, 831, 7181, 1614, 6411, 1854, 6153, 1358, 6667, 1087, 6925, 7235, 6979,
    4932, 5188, 837, 1093, 3142, 2886, 7424, 6706, 4659, 5428, 565, 1334, 3383, 2616, 7982, 6221,
    4174, 5965, 63, 1871, 3918, 2127, 7680, 6450, 4403, 5684, 309, 1590, 3639, 2360, 1091, 835,
    2884, 3140, 6981, 7237, 5190, 4934, 1280, 562, 2611, 3380, 6709, 7478, 5431, 4664, 1802, 47,
    2058, 3851, 6156, 7949, 5900, 4107, 1536, 306, 2355, 3636, 6453, 7734, 5687, 4408, 5649, 5393,
    4379, 4635, 6444, 6700, 7725, 7469, 2361, 2617, 3642, 3386, 1595, 1339, 316, 572, 5907, 5138,
    4134, 4882, 6163, 6946, 7991, 7202, 2073, 2887, 3878, 3143, 1817, 1097, 42, 841, 2577, 2321,
    3355, 3611, 1324, 1580, 557, 301, 5433, 5689, 4666, 4410, 6715, 6459, 7484, 7740, 2818, 2068,
    3074, 3879, 1028, 1812, 772, 56, 5128, 5914, 4872, 4135, 6918, 6170, 7174, 7979, 574, 1289,
    3351, 2584, 7464, 6697, 4650, 5419, 769, 1025, 3136, 2880, 7233, 6977, 4930, 5186, 318, 1545,
    3607, 2328, 7720, 6441, 4394, 5675, 46, 1866, 3915, 2122, 7999, 6220, 4171, 5964, 6718, 7433,
    5399, 4632, 1320, 553, 2602, 3371, 6913, 7169, 5184, 4928, 1089, 833, 2882, 3138, 6462, 7689,
    5655, 4376, 1576, 297, 2346, 3627, 6160, 7983, 5904, 4111, 1806, 13, 2062, 3855, 3645, 261,
    4373, 7702, 3842, 20, 4098, 7939, 3389, 517, 4629, 7446, 3134, 777, 4887, 7192, 2367, 1549,
    5662, 6431, 2065, 1809, 5915, 6171, 2623, 1293, 5406, 6687, 2878, 1033, 5143, 6936, 4121, 5959,
    7974, 6215, 3865, 2121, 21, 1865, 4352, 5682, 7731, 6452, 3637, 2358, 311, 1592, 4921, 5177,
    7226, 6970, 3131, 2875, 828, 1084, 4608, 5426, 7475, 6708, 3381, 2614, 567, 1336, 0, 842, 1867,
    1098, 3903, 3148, 2123, 2892, 7998, 7245, 6222, 6989, 4159, 4943, 5966, 5199, 257, 513, 1600,
    1344, 3649, 3393, 2370, 2626, 7747, 7491, 6468, 6724, 4421, 4677, 5702, 5446, 7184, 7986, 6928,
    6159, 4878, 4109, 5134, 5903, 778, 9, 1034, 1803, 3084, 3853, 2828, 2059, 7425, 7681, 6720,
    6464, 4673, 4417, 5442, 5698, 579, 323, 1348, 1604, 3397, 3653, 2630, 2374, 6172, 6986, 7987,
    7242, 5916, 5196, 4126, 4940, 1824, 1101, 23, 845, 2080, 2895, 3870, 3151, 6401, 6657, 7744,
    7488, 5697, 5441, 4418, 4674, 1603, 1347, 324, 580, 2373, 2629, 3654, 3398, 1040, 1821, 784,
    52, 2830, 2077, 3086, 3871, 6922, 6177, 7178, 7960, 5132, 5921, 4876, 4127, 1281, 1537, 576,
    320, 2625, 2369, 3394, 3650, 6723, 6467, 7492, 7748, 5445, 5701, 4678, 4422, 2085, 2306, 2819,
    2562, 3894, 3588, 3075, 3332, 1829, 1544, 1031, 1288, 69, 262, 775, 518, 5935, 5648, 5135,
    5392, 4150, 4366, 4879, 4622, 6191, 6410, 6923, 6666, 7995, 7692, 7179, 7436, 5650, 5924, 5394,
    5155, 4386, 4149, 4642, 4899, 6471, 6180, 6727, 6984, 7753, 8005, 7497, 7240, 2378, 2094, 2634,
    2891, 3660, 3893, 3404, 3147, 1613, 1838, 1357, 1102, 335, 59, 591, 846, 5140, 5378, 5927,
    5634, 4884, 4612, 4152, 4356, 6938, 6664, 6183, 6408, 7194, 7430, 8006, 7686, 2845, 2576, 2097,
    2320, 3101, 3342, 3896, 3598, 1057, 1290, 1841, 1546, 801, 524, 60, 268, 2578, 2835, 2322,
    2086, 3362, 3091, 3618, 3895, 1351, 1049, 1607, 1830, 585, 793, 329, 70, 5450, 5148, 5706,
    5936, 4684, 4892, 4428, 4151, 6733, 6944, 6477, 6192, 7503, 7200, 7759, 7996, 3110, 4903, 3347,
    4628, 3906, 4162, 3603, 4372, 2854, 5159, 2585, 5402, 2090, 5931, 2329, 5658, 816, 7217, 540,
    7453, 14, 8012, 284, 7709, 1072, 6961, 1312, 6689, 1834, 6187, 1568, 6433, 4643, 3331, 4900,
    3109, 4387, 3587, 4161, 3905, 5448, 2567, 5156, 2853, 5704, 2311, 5928, 2089, 7499, 527, 7214,
    815, 7755, 271, 7950, 76, 6734, 1291, 6958, 1071, 6478, 1547, 6184, 1833, 7697, 7441, 6427,
    6683, 4396, 4652, 5677, 5421, 313, 569, 1594, 1338, 3643, 3387, 2364, 2620, 7936, 7186, 6179,
    6930, 4157, 4898, 5923, 5154, 62, 839, 1864, 1095, 3901, 3145, 2120, 2889, 529, 273, 1307,
    1563, 3372, 3628, 2605, 2349, 7481, 7737, 6714, 6458, 4667, 4411, 5436, 5692, 770, 50, 1026,
    1795, 3076, 3845, 2820, 2051, 7176, 7945, 6920, 6151, 4870, 4101, 5126, 5895, 1553, 1297, 283,
    539, 2348, 2604, 3629, 3373, 6457, 6713, 7738, 7482, 5691, 5435, 4412, 4668, 1811, 1042, 51,
    786, 2067, 2850, 3861, 3106, 6169, 6983, 7959, 7239, 5913, 5193, 4117, 4937, 6673, 6417, 7451,
    7707, 5420, 5676, 4653, 4397, 1337, 1593, 570, 314, 2619, 2363, 3388, 3644, 6914, 6164, 7170,
    7988, 5124, 5908, 4868, 4118, 1032, 1818, 776, 24, 2822, 2074, 3078, 3862, 7698, 7937, 7442,
    7203, 6434, 6205, 6690, 6947, 4423, 4158, 4679, 4936, 5705, 5949, 5449, 5192, 330, 17, 586,
    843, 1612, 1855, 1356, 1099, 3661, 3902, 3405, 3150, 2383, 2111, 2639, 2894, 7188, 7426, 8000,
    7682, 6932, 6660, 6166, 6404, 4890, 4616, 4120, 4360, 5146, 5382, 5910, 5638, 797, 528, 27,
    272, 1053, 1294, 1823, 1550, 3105, 3338, 3864, 3594, 2849, 2572, 2079, 2316, 530, 787, 274, 64,
    1314, 1043, 1570, 1813, 3399, 3097, 3655, 3863, 2633, 2841, 2377, 2069, 7498, 7196, 7754, 7963,
    6732, 6940, 6476, 6174, 4685, 4896, 4429, 4119, 5455, 5152, 5711, 5918, 6181, 6402, 6915, 6658,
    8001, 7684, 7171, 7428, 5925, 5640, 5127, 5384, 4137, 4358, 4871, 4614, 1839, 1552, 1039, 1296,
    44, 270, 783, 526, 2095, 2314, 2827, 2570, 3881, 3596, 3083, 3340, 1554, 1828, 1298, 1059, 290,
    65, 546, 803, 2375, 2084, 2631, 2888, 3657, 3880, 3401, 3144, 6474, 6190, 6730, 6987, 7756,
    7980, 7500, 7243, 5709, 5934, 5453, 5198, 4431, 4136, 4687, 4942, 1044, 1282, 1831, 1538, 788,
    516, 66, 260, 2842, 2568, 2087, 2312, 3098, 3334, 3883, 3590, 6941, 6672, 6193, 6416, 7197,
    7438, 7981, 7694, 5153, 5386, 5937, 5642, 4897, 4620, 4139, 4364, 6674, 6931, 6418, 6182, 7458,
    7187, 7714, 8002, 5447, 5145, 5703, 5926, 4681, 4889, 4425, 4138, 1354, 1052, 1610, 1840, 588,
    796, 332, 45, 2637, 2848, 2381, 2096, 3407, 3104, 3663, 3882, 2099, 5940, 2323, 5652, 2837,
    5142, 2579, 5396, 3908, 4164, 3609, 4378, 3093, 4886, 3353, 4634, 1843, 6196, 1564, 6429, 1054,
    6943, 1308, 6685, 10, 8013, 288, 7713, 798, 7199, 544, 7457, 5667, 2307, 5888, 2098, 5411,
    2563, 5181, 2821, 4424, 3591, 4163, 3907, 4680, 3335, 4925, 3077, 6475, 1551, 6144, 1842, 6731,
    1295, 6975, 1037, 7758, 267, 7946, 77, 7502, 523, 7231, 781, 5158, 2855, 5395, 2580, 5943,
    2104, 5651, 2324, 4902, 3111, 4633, 3354, 4166, 3910, 4377, 3610, 6960, 1073, 6684, 1309, 6199,
    1848, 6428, 1565, 7216, 817, 7456, 545, 7948, 79, 7712, 289, 2595, 5379, 2852, 5157, 2339,
    5635, 2101, 5942, 3400, 4615, 3108, 4901, 3656, 4359, 3909, 4165, 1355, 6671, 1070, 6959, 1611,
    6415, 1845, 6198, 590, 7435, 814, 7215, 334, 7691, 12, 8015, 4142, 4938, 5963, 5194, 7989,
    7244, 6219, 6988, 3886, 3149, 2126, 2893, 40, 847, 1870, 1103, 4353, 4609, 5696, 5440, 7745,
    7489, 6466, 6722, 3651, 3395, 2372, 2628, 325, 581, 1606, 1350, 4146, 4354, 4867, 4610, 5893,
    5636, 5123, 5380, 8003, 7688, 7175, 7432, 6149, 6406, 6919, 6662, 3890, 3600, 3087, 3344, 2061,
    2318, 2831, 2574, 57, 266, 779, 522, 1805, 1548, 1035, 1292, 3601, 3345, 2331, 2587, 300, 556,
    1581, 1325, 4409, 4665, 5690, 5434, 7739, 7483, 6460, 6716, 3876, 3090, 2083, 2834, 53, 802,
    1827, 1058, 4132, 4935, 5960, 5191, 7976, 7241, 6216, 6985, 7715, 259, 7938, 18, 7459, 515,
    7229, 773, 6472, 1543, 6206, 1801, 6728, 1287, 6973, 1029, 4427, 3599, 4113, 3857, 4683, 3343,
    4927, 3085, 5710, 2315, 5950, 2057, 5454, 2571, 5183, 2829, 7206, 807, 7443, 532, 7940, 34,
    7699, 276, 6950, 1063, 6681, 1306, 6186, 1835, 6425, 1562, 4912, 3121, 4636, 3357, 4141, 3885,
    4380, 3613, 5168, 2865, 5408, 2593, 5930, 2091, 5664, 2337, 547, 7427, 804, 7205, 291, 7683, 4,
    7970, 1352, 6663, 1060, 6949, 1608, 6407, 1832, 6185, 3403, 4623, 3118, 4911, 3659, 4367, 3884,
    4140, 2638, 5387, 2862, 5167, 2382, 5643, 2088, 5929, 6195, 1844, 6419, 1556, 6933, 1046, 6675,
    1300, 7944, 71, 7705, 282, 7189, 790, 7449, 538, 5939, 2100, 5660, 2333, 5150, 2847, 5404,
    2589, 4154, 3898, 4384, 3617, 4894, 3103, 4640, 3361, 1571, 6403, 1792, 6194, 1315, 6659, 1085,
    6917, 328, 7687, 8, 8007, 584, 7431, 829, 7173, 2379, 5647, 2048, 5938, 2635, 5391, 2879, 5133,
    3662, 4363, 3897, 4153, 3406, 4619, 3135, 4877, 1062, 6951, 1299, 6676, 1847, 6200, 1555, 6420,
    806, 7207, 537, 7450, 6, 8009, 281, 7706, 2864, 5169, 2588, 5405, 2103, 5944, 2332, 5661, 3120,
    4913, 3360, 4641, 3900, 4156, 3616, 4385, 6691, 1283, 6948, 1061, 6435, 1539, 6197, 1846, 7496,
    519, 7204, 805, 7752, 263, 7942, 73, 5451, 2575, 5166, 2863, 5707, 2319, 5941, 2102, 4686,
    3339, 4910, 3119, 4430, 3595, 4155, 3899, 3125, 822, 4919, 7224, 3364, 549, 4646, 7463, 3846,
    26, 4102, 7943, 3620, 293, 4390, 7719, 2869, 1078, 5175, 6968, 2606, 1327, 5424, 6705, 2107,
    1851, 5948, 6204, 2350, 1583, 5680, 6449, 3646, 2313, 279, 1560, 4392, 5673, 7722, 6443, 3859,
    2066, 38, 1810, 4115, 5922, 7957, 6178, 3390, 2569, 535, 1304, 4648, 5417, 7466, 6699, 3089,
    2833, 795, 1051, 4908, 5164, 7213, 6957, 4669, 7429, 3349, 534, 4864, 7218, 3123, 820, 4413,
    7685, 3605, 278, 4104, 7962, 3848, 7, 5439, 6669, 2590, 1311, 5120, 6962, 2867, 1076, 5695,
    6413, 2334, 1567, 5945, 6201, 2106, 1850, 2113, 1857, 5954, 6210, 2340, 1573, 5670, 6439, 2856,
    1065, 5162, 6955, 2596, 1317, 5414, 6695, 3854, 29, 4110, 7951, 3630, 303, 4400, 7729, 3112,
    809, 4906, 7211, 3374, 559, 4656, 7473, 3139, 2883, 836, 1092, 4933, 5189, 7238, 6982, 3328,
    2610, 563, 1332, 4661, 5430, 7479, 6712, 3872, 2125, 48, 1869, 4128, 5967, 7966, 6223, 3584,
    2354, 307, 1588, 4405, 5686, 7735, 6456, 5693, 6405, 2325, 1558, 5889, 6145, 2112, 1856, 5437,
    6661, 2581, 1302, 5182, 6921, 2839, 1048, 4415, 7693, 3614, 287, 4112, 7965, 3856, 15, 4671,
    7437, 3358, 543, 4926, 7177, 3095, 792, 5173, 6966, 2871, 1080, 5412, 6693, 2598, 1319, 5957,
    6213, 2118, 1862, 5668, 6437, 2342, 1575, 4917, 7222, 3127, 824, 4654, 7471, 3376, 561, 4108,
    7969, 3852, 11, 4398, 7727, 3632, 305, 4670, 5385, 7447, 6680, 3368, 2601, 554, 1323, 4865,
    5121, 7232, 6976, 3137, 2881, 834, 1090, 4414, 5641, 7703, 6424, 3624, 2345, 298, 1579, 4124,
    5962, 7984, 6218, 3868, 2124, 30, 1868, 2621, 1285, 5397, 6678, 2816, 1074, 5171, 6964, 2365,
    1541, 5653, 6422, 2115, 1859, 5956, 6212, 3391, 525, 4638, 7455, 3072, 818, 4915, 7220, 3647,
    269, 4382, 7711, 3850, 33, 4106, 7947,
];

static LUT: LazyLock<Vec<&'static [u16]>> = LazyLock::new(|| vec![&LUT_2, &LUT_3, &LUT_4, &LUT_5]);

macro_rules! hilbert_index {
    ($name:ident, $t:ty, $l: literal) => {
        pub fn $name(point: &[$t]) -> Vec<u8> {
            let n = point.len();
            let states = &LUT[n - 2];
            let num_bits = $l * n;
            let num_bytes = (num_bits + 7) / 8;
            let mut result = vec![0u8; num_bytes];
            let initial_offset = (num_bytes * 8) - num_bits;
            let mut current_state = 0;

            for i in 0..$l {
                let mut z = 0;
                point.iter().for_each(|v| {
                    z = (z << 1) | ((*v >> ($l - 1 - i)) & 1) as u8;
                });

                let value = states[current_state as usize * (1 << n) + z as usize];
                let new_bits = (value >> 8) as u8;
                let next_state = (value & 255) as u8;
                let offset = initial_offset + (i * n);

                let mut bits = (new_bits as u16) << (16 - n);
                let mut remaining_bits = n;
                let mut key_index = offset / 8;
                let mut key_offset = offset - (key_index * 8);

                while remaining_bits > 0 {
                    result[key_index] |= (bits >> (8 + key_offset)) as u8;
                    remaining_bits -= (8 - key_offset).min(remaining_bits);
                    bits <<= 8 - key_offset;
                    key_offset = 0;
                    key_index += 1;
                }
                current_state = next_state;
            }

            result
        }
    };
}

hilbert_index!(hilbert_index_u8, u8, 8);
hilbert_index!(hilbert_index_u16, u16, 16);
hilbert_index!(hilbert_index_u32, u32, 32);
hilbert_index!(hilbert_index_u64, u64, 64);

pub fn hilbert_index_vec(point: &[&[u8]], width: usize) -> Vec<u8> {
    let n = point.len();
    let states = LUT[n - 2];
    let num_bits = width * n;
    let num_bytes = (num_bits + 7) / 8;
    let mut result = vec![0u8; num_bytes];
    let initial_offset = (num_bytes * 8) - num_bits;
    let mut current_state = 0;

    for i in 0..width {
        let mut z = 0;

        for v in point {
            let byte_index = i / 8;
            let bit_index = 7 - (i % 8);

            let byte = *v.get(byte_index).unwrap_or(&0);
            z = (z << 1) | ((byte >> bit_index) & 1);
        }

        let value = states[current_state as usize * (1 << n) + z as usize];
        let new_bits = (value >> 8) as u8;
        let next_state = (value & 0xFF) as u8;
        let offset = initial_offset + (i * n);

        let mut bits = (new_bits as u16) << (16 - n);
        let mut remaining_bits = n;
        let mut key_index = offset / 8;
        let mut key_offset = offset % 8;

        while remaining_bits > 0 {
            result[key_index] |= (bits >> (8 + key_offset)) as u8;
            remaining_bits -= (8 - key_offset).min(remaining_bits);
            bits <<= 8 - key_offset;
            key_offset = 0;
            key_index += 1;
        }
        current_state = next_state;
    }

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::VecDeque;

    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;

    use super::*;

    pub fn hilbert_compact_state_list(dimension: usize) -> Result<Vec<u16>> {
        let state_map = generate_state_map(dimension)?;
        let nums = 1 << dimension;
        let mut state_list = vec![0u16; nums * state_map.len()];
        for (state_idx, state) in state_map {
            let state_start_idx = state_idx as usize * nums;

            for (y, x1, state) in &state.point_states {
                state_list[state_start_idx + *x1 as usize] = ((*y as u16) << 8) | *state as u16;
            }
        }
        Ok(state_list)
    }

    #[inline]
    fn left_shift(n: u8, i: u8, shift: u8) -> u8 {
        ((i << shift) | (i >> (n - shift))) & ((1 << n) - 1)
    }

    #[derive(Clone, Debug)]
    struct State {
        id: u8,

        x2: u8,
        dy: u8,
        // (y,x1,state)
        point_states: Vec<(u8, u8, u8)>,
    }

    fn get_x2_gray_codes(n: u8) -> Vec<u8> {
        if n == 1 {
            return vec![0, 1, 0, 1];
        }

        let mask = 1 << (n - 1);
        let mut base = get_x2_gray_codes(n - 1);

        let last_index = base.len() - 1;
        base[last_index] = base[last_index - 1] + mask;

        let len = base.len() * 2;
        let mut result = vec![0; len];
        for i in 0..base.len() {
            result[i] = base[i];
            result[len - 1 - i] = base[i] ^ mask;
        }

        result
    }

    fn generate_state_map(dimension: usize) -> Result<HashMap<u8, State>> {
        struct GrayCodeEntry {
            y: u8,
            x1: u8,
            x2: u8,
            dy: u8,
        }
        if !(2..=5).contains(&dimension) {
            return Err(ErrorCode::Internal(
                "Only support dimension between 2 and 5",
            ));
        }
        let n = dimension as u8;

        let x2s = get_x2_gray_codes(n);
        let len = 1u8 << n;
        let gray_codes = (0..len)
            .map(|i| {
                let idx = (i << 1) as usize;
                let x21 = x2s[idx];
                let x22 = x2s[idx + 1];
                let dy = x21 ^ x22;

                GrayCodeEntry {
                    y: i,
                    x1: i ^ (i >> 1),
                    x2: x21,
                    dy: n - 1 - dy.trailing_zeros() as u8,
                }
            })
            .collect::<Vec<_>>();
        let row_len = gray_codes.len();
        assert!(row_len < 256);

        let mut state_map: HashMap<u8, State> = HashMap::new();
        let mut list: VecDeque<u8> = VecDeque::new();
        let mut next_state_num = 1;

        let initial_state = State {
            id: 0,
            x2: 0,
            dy: 0,
            point_states: gray_codes.iter().map(|r| (r.y, r.x1, 0)).collect(),
        };
        state_map.insert(0, initial_state);

        for entry in &gray_codes {
            let s_id = state_map
                .values()
                .find_map(|s| (s.x2 == entry.x2 && s.dy == entry.dy).then_some(s.id));
            if let Some(id) = s_id {
                let initial_state = state_map.get_mut(&0).unwrap();
                initial_state.point_states[entry.y as usize].2 = id;
            } else {
                let initial_state = state_map.get_mut(&0).unwrap();
                initial_state.point_states[entry.y as usize].2 = next_state_num;
                let new_state = State {
                    id: next_state_num,
                    x2: entry.x2,
                    dy: entry.dy,
                    point_states: Vec::new(),
                };
                state_map.insert(next_state_num, new_state);
                list.push_back(next_state_num);
                next_state_num += 1;
            }
        }

        let rows_len = row_len as u8;
        while let Some(current_state_id) = list.pop_front() {
            let mut current_state = state_map.get(&current_state_id).unwrap().clone();
            current_state.point_states = (0..rows_len).map(|r| (r, 0, 0)).collect();

            for i in 0..rows_len {
                let j = left_shift(n, i ^ current_state.x2, current_state.dy);
                let initial_state = state_map.get(&0).unwrap();
                let p = initial_state
                    .point_states
                    .iter()
                    .find(|(_, x1, _)| *x1 == j)
                    .expect("PointState not found");

                let current_point_state = &mut current_state.point_states[p.0 as usize];
                current_point_state.1 = i;

                let (x2, dy) = state_map
                    .get(&p.2)
                    .map(|v| {
                        let right_shift = ((v.x2 >> current_state.dy)
                            | (v.x2 << (n - current_state.dy)))
                            & (len - 1);
                        (
                            right_shift ^ current_state.x2,
                            (v.dy + current_state.dy) % n,
                        )
                    })
                    .expect("State not found");

                let s_id = state_map
                    .values()
                    .find_map(|s| (s.x2 == x2 && s.dy == dy).then_some(s.id));
                if let Some(id) = s_id {
                    current_point_state.2 = id;
                } else {
                    current_point_state.2 = next_state_num;
                    let new_state = State {
                        id: next_state_num,
                        x2,
                        dy,
                        point_states: vec![],
                    };
                    state_map.insert(next_state_num, new_state);
                    list.push_back(next_state_num);
                    next_state_num += 1;
                }
            }

            state_map.insert(current_state_id, current_state);
        }

        Ok(state_map)
    }

    fn hilbert_decompress_state_list(dimension: usize) -> Result<Vec<u16>> {
        let state_map = generate_state_map(dimension)?;
        let nums = 1 << dimension;
        let mut state_list = vec![0u16; nums * state_map.len()];
        for (state_idx, state) in state_map {
            let state_start_idx = state_idx as usize * nums;

            for (y, x1, state) in &state.point_states {
                state_list[state_start_idx + *y as usize] = ((*x1 as u16) << 8) | *state as u16;
            }
        }
        Ok(state_list)
    }

    macro_rules! hilbert_points {
        ($name:ident, $t:ty, $l: literal) => {
            fn $name(key: &[u8], n: usize, states: &[u16]) -> Vec<$t> {
                let mut result = vec![0; n];
                let initial_offset = key.len() * 8 - $l * n;
                let mut current_state = 0;

                for i in 0..$l {
                    let offset = initial_offset + i * n;
                    let mut h = 0;
                    let mut remaining_bits = n;
                    let mut key_index = offset / 8;
                    let mut key_offset = offset - (key_index * 8);

                    while remaining_bits > 0 {
                        let bits_from_idx = remaining_bits.min(8 - key_offset);
                        let new_int = key[key_index] >> (8 - key_offset - bits_from_idx);
                        h = (h << bits_from_idx) | (new_int & ((1 << bits_from_idx) - 1));

                        remaining_bits -= bits_from_idx;
                        key_offset = 0;
                        key_index += 1;
                    }

                    let value = states[current_state as usize * (1 << n) + h as usize];
                    let z = (value >> 8) as u8;
                    let next_state = (value & 255) as u8;
                    for (j, item) in result.iter_mut().enumerate() {
                        let v = (z >> (n - 1 - j)) & 1;
                        *item = (*item << 1) | (v as $t);
                    }

                    current_state = next_state;
                }

                result
            }
        };
    }

    // hilbert_points!(hilbert_points_u8, u8, 8);
    // hilbert_points!(hilbert_points_u16, u16, 16);
    hilbert_points!(hilbert_points_u32, u32, 32);
    // hilbert_points!(hilbert_points_u64, u64, 64);

    use crate::row::FixedLengthEncoding;
    #[test]
    fn tests() -> Result<()> {
        let n = 5;
        let state_list = hilbert_compact_state_list(n)?;
        println!("state_list len: {}", state_list.len(),);

        let point = vec![1, 2, 3, 4, 6];
        let key = hilbert_index_u32(&point);
        println!("res: {:?}", key);

        let res = point
            .iter()
            .map(|v| v.encode())
            .collect::<Vec<_>>();
        let vec_of_slices: Vec<&[u8]> = res
            .iter()  // Iterate over references to each [u8; 4]
            .map(|array| &array[..]) // Convert &[u8; 4] to &[u8]
            .collect();
        let key1 = hilbert_index_vec(&vec_of_slices, 64);
        println!("key: {:?}", key1);
        // [17, 77, 230, 57, 142, 99, 152, 230, 57, 142, 99, 152, 230, 57, 142, 99, 152, 230, 57, 142, 99, 152]

        let state_list = hilbert_decompress_state_list(n)?;
        println!("state_list len: {}", state_list.len(),);
        let res = hilbert_points_u32(&key, n, &state_list);
        println!("res: {:?}", res);

        Ok(())
    }
}
