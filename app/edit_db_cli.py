#!/usr/bin/env python3
"""
CLI интерфейс для редактирования базы данных проб.
"""

import argparse
import sys
from app.database_editor import DatabaseEditor

def main():
    parser = argparse.ArgumentParser(
        description='Редактор базы данных проб',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s --data data.json --add-field "лаборант" --value "Иванов"
  %(prog)s --data data.json --update-field "комментарий" --value "проверено" --tag "важный"
  %(prog)s --data data.json --stats
        """
    )
    
    # Основные аргументы
    parser.add_argument('--data', required=True, help='Путь к JSON файлу с данными')
    
    # Операции
    operation_group = parser.add_mutually_exclusive_group(required=True)
    operation_group.add_argument('--add-field', help='Добавить новое поле')
    operation_group.add_argument('--update-field', help='Изменить существующее поле')
    operation_group.add_argument('--stats', action='store_true', help='Показать статистику')
    operation_group.add_argument('--list-tags', action='store_true', help='Показать все теги')
    operation_group.add_argument('--remove-field', help='Удалить поле')
    
    # Дополнительные параметры
    parser.add_argument('--value', help='Значение для установки')
    parser.add_argument('--tag', nargs='+', help='Тег для фильтрации')
    parser.add_argument('--match-all', action='store_true', help='Все теги должны совпадать')
    parser.add_argument('--type', choices=['string', 'number', 'boolean', 'auto'], 
                       default='auto', help='Тип значения')
    parser.add_argument('--default', help='Значение по умолчанию (для добавления поля)')
    parser.add_argument('--no-confirm', action='store_true', help='Не запрашивать подтверждение')
    
    args = parser.parse_args()
    
    try:
        # Инициализируем редактор
        editor = DatabaseEditor(args.data)
        
        # Выполняем операцию
        if args.add_field:
            value = args.value if args.value else args.default
            result = editor.add_new_field(
                field_name=args.add_field,
                default_value=value,
                value_type=args.type
            )
            print(f"✅ Добавлено поле '{result['field']}' для {result['added']} проб")
            
        elif args.update_field:
            if not args.value:
                print("❌ Ошибка: необходимо указать --value для обновления поля")
                sys.exit(1)
            
            if args.tag:
                result = editor.update_probes_by_tag(
                    field_name=args.update_field,
                    new_value=args.value,
                    tag_filter=args.tag,
                    match_all=args.match_all,
                    value_type=args.type
                )
                print(f"✅ Обновлено поле '{result['field']}' для {result['updated']} проб с тегом(ами): {args.tag}")
            else:
                result = editor.update_all_probes_field(
                    field_name=args.update_field,
                    new_value=args.value,
                    value_type=args.type
                )
                print(f"✅ Обновлено поле '{result['field']}' для {result['updated']} проб")
                
        elif args.remove_field:
            result = editor.remove_field(
                field_name=args.remove_field,
                confirm=not args.no_confirm
            )
            if result['success']:
                print(f"✅ Удалено поле '{result['field']}' из {result['removed']} проб")
            else:
                print(f"❌ Ошибка: {result['error']}")
                
        elif args.stats:
            probes = editor.get_probes()
            print(f"📊 Статистика базы данных:")
            print(f"   Всего проб: {len(probes)}")
            
            # Собираем все уникальные поля
            all_fields = set()
            for probe in probes:
                all_fields.update(probe.keys())
            
            print(f"   Уникальных полей: {len(all_fields)}")
            print(f"   Примеры полей: {sorted(list(all_fields))[:10]}...")
            
            if len(probes) > 0:
                # Показываем статистику для нескольких полей
                sample_probe = probes[0]
                for field in ['name', 'status_id', 'priority', 'sample_mass']:
                    if field in sample_probe:
                        stats = editor.get_field_statistics(field)
                        print(f"\n   📈 Поле '{field}':")
                        print(f"      Есть у: {stats['has_field']} проб")
                        if 'min' in stats:
                            print(f"      Диапазон: {stats['min']} - {stats['max']}")
                            print(f"      Среднее: {stats['mean']:.2f}")
                            
        elif args.list_tags:
            probes = editor.get_probes()
            all_tags = set()
            for probe in probes:
                tags = probe.get('tags', [])
                all_tags.update(tags)
            
            print(f"🏷️  Всего уникальных тегов: {len(all_tags)}")
            print(f"   Теги: {sorted(list(all_tags))}")
        
        print(f"\n📝 Файл данных: {args.data}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()