from collections import defaultdict

from algo.routing_logics.routing_util import merge_overlapping_ranges
from algo.routing_logics.coloring_util import color_bipartite_multigraph, color_bipartite_multigraph_helper
from algo.routing_logics.routing_plot_util import plot_edge_coloring    

from pprint import pprint   
import random 

# Example usage
if __name__ == "__main__":
    # print("Example usage of coloring a bipartite multigraph")
    
    # # Example 1: 3 edges between u1-v1 and u2-v2
    # edges = [('1_l', '7_r', 1), ('7_l', '4_r', 2), ('4_l', '10_r', 3), ('10_l', '0_r', 4), ('0_l', '5_r', 5), ('5_l', '8_r', 6), ('8_l', '6_r', 7), ('6_l', '9_r', 8), ('9_l', '4_r', 9), ('4_l', '11_r', 10), ('11_l', '3_r', 11), ('3_l', '4_r', 12), ('4_l', '0_r', 13), ('0_l', '7_r', 14), ('7_l', '2_r', 15), ('2_l', '9_r', 16), ('9_l', '3_r', 17), ('3_l', '5_r', 18), ('5_l', '10_r', 19), ('10_l', '0_r', 20), ('0_l', '9_r', 21), ('9_l', '5_r', 22), ('5_l', '11_r', 23), ('11_l', '3_r', 24), ('3_l', '1_r', 25), ('1_l', '2_r', 26), ('2_l', '1_r', 27), ('1_l', '3_r', 28), ('3_l', '6_r', 29), ('6_l', '7_r', 30), ('7_l', '8_r', 31), ('8_l', '11_r', 32), ('11_l', '1_r', 33), ('1_l', '6_r', 34), ('6_l', '11_r', 35), ('11_l', '10_r', 36), ('10_l', '2_r', 37), ('2_l', '8_r', 38), ('8_l', '6_r', 39), ('6_l', '4_r', 40), ('4_l', '0_r', 41), ('0_l', '9_r', 42), ('9_l', '2_r', 43), ('2_l', '5_r', 44), ('5_l', '8_r', 45), ('8_l', '1_r', 46)]
    
    # i = 0 
    # while True:    
    #     i += 1  
        
    #     random.seed(i)  
    #     random.shuffle(edges)   

    #     edges = [(r[0], r[1]) for r in edges]   
    #     colors = color_bipartite_multigraph(edges)
        
    #     # for each color, store the edges
    #     color_edge_map = defaultdict(list)
    #     colored_edges = 0 
    #     for idx, (u, v) in enumerate(edges):    
    #         color_edge_map[colors[idx]].append(idx)
    #         colored_edges += 1
            
    #     pprint(color_edge_map)  
        
    #     used_color_count = len(set(colors)) 
    #     print(f"[{i}] Used {used_color_count} colors for {len(edges)} edges") 
        
    #     if used_color_count == 4:
    #         break
    
    
    # hash_to_time_ranges = {'ac325c7e80c1261a53cda071fc166e1e': [(200, 213), (214, 227), (228, 241), (242, 255), (256, 269), (270, 283), (284, 297), (298, 311), (312, 325), (326, 339), (340, 350), (354, 367), (368, 381), (382, 395), (396, 409), (410, 423), (424, 437), (438, 451), (452, 465), (466, 479), (480, 493), (494, 507), (708, 721), (722, 735), (736, 749), (750, 763), (764, 777), (778, 791), (792, 805), (806, 819), (820, 833), (834, 847), (848, 861), (862, 875), (876, 889), (890, 903), (904, 917), (918, 931), (932, 945), (946, 959), (960, 973), (974, 987), (988, 1001), (1002, 1015)], 'd013df15ae7ea8d10cc7acf9f29c5f87': [(410, 423), (424, 437), (438, 451), (452, 465), (466, 479), (480, 493), (494, 507), (508, 521), (522, 535), (536, 549), (550, 563), (564, 577), (578, 591), (592, 605), (606, 619), (620, 633), (634, 647), (648, 661), (662, 675), (676, 689), (690, 703), (704, 717)], 'f812aba498373f2e51807406aca0a97c': [(200, 226), (227, 253), (254, 280), (281, 307), (308, 334), (335, 350), (362, 388), (389, 415), (416, 442), (443, 469), (470, 496), (497, 523), (524, 550), (551, 577), (578, 604), (605, 631), (632, 658), (659, 685), (686, 712), (713, 739), (740, 766), (767, 793)], '1e2a2d258077a845944938f8e84a3d19': [(260, 273), (274, 287), (288, 301), (302, 315), (316, 329), (330, 343), (344, 350), (358, 371), (372, 385), (386, 399), (400, 413), (414, 427), (428, 441), (442, 455), (456, 469), (470, 483), (484, 497), (498, 511), (512, 525), (526, 539), (540, 553), (554, 567), (768, 781), (782, 795), (796, 809), (810, 823), (824, 837), (838, 851), (852, 865), (866, 879), (880, 893), (894, 907), (908, 921), (922, 935), (936, 949), (950, 963), (964, 977), (978, 991), (992, 1005), (1006, 1019), (1020, 1033), (1034, 1047), (1048, 1061), (1062, 1075)]}
    
    # merged_ranges = merge_overlapping_ranges(hash_to_time_ranges, "merged_ranges.png")
    
    # pprint(merged_ranges)
    
    
    
    edges = [('0_l', '1_r', 1), ('0_l', '1_r', 2), ('0_l', '1_r', 3), ('0_l', '1_r', 4), ('0_l', '3_r', 5), ('0_l', '3_r', 6), ('0_l', '3_r', 7), ('0_l', '3_r', 8), ('1_l', '0_r', 9), ('1_l', '0_r', 10), ('1_l', '0_r', 11), ('1_l', '0_r', 12), ('1_l', '2_r', 13), ('1_l', '2_r', 14), ('1_l', '2_r', 15), ('1_l', '2_r', 16), ('2_l', '3_r', 17), ('2_l', '3_r', 18), ('2_l', '3_r', 19), ('2_l', '3_r', 20), ('2_l', '5_r', 21), ('2_l', '5_r', 22), ('2_l', '5_r', 23), ('2_l', '5_r', 24), ('3_l', '1_r', 25), ('3_l', '1_r', 26), ('3_l', '1_r', 27), ('3_l', '1_r', 28), ('3_l', '4_r', 29), ('3_l', '4_r', 30), ('3_l', '4_r', 31), ('3_l', '4_r', 32), ('4_l', '0_r', 33), ('4_l', '0_r', 34), ('4_l', '0_r', 35), ('4_l', '0_r', 36), ('5_l', '2_r', 37), ('5_l', '2_r', 38), ('5_l', '2_r', 39), ('5_l', '2_r', 40), ('0_l', '2_r', 41), ('0_l', '2_r', 42), ('0_l', '2_r', 43), ('0_l', '2_r', 44), ('0_l', '4_r', 45), ('0_l', '4_r', 46), ('0_l', '4_r', 47), ('0_l', '4_r', 48), ('2_l', '0_r', 49), ('2_l', '0_r', 50), ('2_l', '0_r', 51), ('2_l', '0_r', 52), ('2_l', '5_r', 53), ('2_l', '5_r', 54), ('2_l', '5_r', 55), ('2_l', '5_r', 56), ('3_l', '2_r', 57), ('3_l', '2_r', 58), ('3_l', '2_r', 59), ('3_l', '2_r', 60), ('3_l', '5_r', 61), ('3_l', '5_r', 62), ('3_l', '5_r', 63), ('3_l', '5_r', 64), ('4_l', '3_r', 65), ('4_l', '3_r', 66), ('4_l', '3_r', 67), ('4_l', '3_r', 68), ('4_l', '3_r', 69), ('4_l', '3_r', 70), ('4_l', '3_r', 71), ('4_l', '3_r', 72), ('5_l', '0_r', 73), ('5_l', '0_r', 74), ('5_l', '0_r', 75), ('5_l', '0_r', 76), ('5_l', '4_r', 77), ('5_l', '4_r', 78), ('5_l', '4_r', 79), ('5_l', '4_r', 80)]
    
    edge_colors, max_degree = color_bipartite_multigraph(edges) 
    # plot_edge_coloring(edges, edge_colors, "edge_coloring.png")  
    
    # print("edge_colors: ", edge_colors)   
    # print("max_degree: ", max_degree)
    colors_to_idx = defaultdict(list) 
    for idx, color in edge_colors.items(): 
        colors_to_idx[color].append(idx)    
    # shuffle the edges 
    used_color_count = len(set(edge_colors.values()))   
    print("colors: {}".format(used_color_count))

    # round = 1
    # while used_color_count > max_degree:
    #     edge_colors, max_degree = color_bipartite_multigraph(edges) 
    #     # plot_edge_coloring(edges, edge_colors, f"edge_coloring_{round}.png")    
        
    #     colors_to_idx = defaultdict(list) 
    #     for idx, color in edge_colors.items(): 
    #         colors_to_idx[color].append(idx)    
    #     used_color_count = len(set(edge_colors.values()))
    #     print("colors: {}".format(used_color_count))

    #     round += 1  
    
    # print("-----------------")