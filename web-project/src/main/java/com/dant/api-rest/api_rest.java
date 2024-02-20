@RestController
@RequestMapping("/api")
public class ApiController {

    @Autowired
    private DatabaseService databaseService;

    @GetMapping("/select")
    public ResponseEntity<?> selectData(@RequestParam("table") String table,
                                        @RequestParam("columns") List<String> columns,
                                        @RequestParam("conditions") Map<String, String> conditions) {
        try {
            List<Map<String, Object>> results = databaseService.select(table, columns, conditions);
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Erreur lors de l'exécution de la commande SELECT.");
        }
    }
}
